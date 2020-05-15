using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Avro.Specific;
using Confluent.Kafka;
using Newtonsoft.Json.Linq;

namespace Confluent.SchemaRegistry.Serdes.Avro
{
    /// <summary>
    ///     (async) Avro deserializer. This deserializer is intended to be used
    ///     with SpecificRecord types generated using the avrogen.exe. This
    ///     deserializer can be used with topics containing more than one
    ///     schema. See <see cref="DeserializeAsync"/> for more implementation
    ///     details
    /// </summary>
    /// <remarks>
    ///     Serialization format:
    ///       byte 0:           Magic byte use to identify the protocol format.
    ///       bytes 1-4:        Unique global id of the Avro schema that was used for encoding (as registered in Confluent Schema Registry), big endian.
    ///       following bytes:  The serialized data.
    /// </remarks>
    public class DynamicSpecificRecordDeserializer : IAsyncDeserializer<ISpecificRecord>
    {
        private readonly ISchemaRegistryClient _schemaRegistryClient;
        private readonly IReadOnlyCollection<KeyValuePair<string, string>> _config;
        private readonly Dictionary<int, dynamic> _deserializerCache = new Dictionary<int, dynamic>();

        /// <summary>
        ///     Initialize a new DynamicSpecificRecordDeserializer instance.
        /// </summary>
        /// <param name="schemaRegistryClient">
        ///     An implementation of ISchemaRegistryClient used for
        ///     communication with Confluent Schema Registry.
        /// </param>
        /// <param name="config">
        ///     Deserializer configuration properties (refer to 
        ///     <see cref="AvroDeserializerConfig" />).
        /// </param>
        public DynamicSpecificRecordDeserializer(ISchemaRegistryClient schemaRegistryClient,
            IReadOnlyCollection<KeyValuePair<string, string>> config = null)
        {
            // There is some config validation that happens so make sure we can construct one
            // ReSharper disable once ObjectCreationAsStatement
            new AvroDeserializer<ISpecificRecord>(schemaRegistryClient, config);

            _schemaRegistryClient = schemaRegistryClient;
            _config = config;
        }

        /// <summary>
        ///     Deserialize a byte array in to an instance of type
        ///     <see cref="ISpecificRecord" />. This is done by finding the
        ///     SchemaId from the provided <paramref name="data"/>. If the
        ///     schema has not been seen before the <see cref="ISchemaRegistryClient"/>
        ///     is used to download the schema. The schema is used to attempt
        ///     to load type information based on the namespace and name.
        ///     After getting the Namespace and Name of the SpecificRecord
        ///     the type is loaded and a concrete instance of
        ///     <see cref="AvroDeserializer{T}"/> is constructed and cached.
        ///     Using the cached deserializer the data is then deserialized 
        ///     to an instance of <see cref="ISpecificRecord" />
        /// </summary>
        /// <param name="data">
        ///     The raw byte data to deserialize.
        /// </param>
        /// <param name="isNull">
        ///     True if this is a null value.
        /// </param>
        /// <param name="context">
        ///     Context relevant to the deserialize operation.
        /// </param>
        /// <returns>
        ///     A <see cref="System.Threading.Tasks.Task" /> that completes
        ///     with the deserialized value.
        /// </returns>
        /// <exception cref="System.IO.InvalidDataException">
        ///     Thrown when <paramref name="data"/> does not have a length of
        ///     at least 5 bytes.
        /// </exception>
        /// <exception cref="System.IO.InvalidDataException">
        ///     Thrown when the SchemaId indicated by <paramref name="data"/>
        ///     has a namespace + name which does not match a defined type
        /// </exception>
        public async Task<ISpecificRecord> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            var dataArray = data.ToArray();

            if (dataArray.Length < 5)
                throw new InvalidDataException($"Expecting data framing of length 5 bytes or more but total data size is {dataArray.Length} bytes");

            if (dataArray[0] != 0)
                throw new InvalidDataException($"Expecting data with Confluent Schema Registry framing. Magic byte was {dataArray[0]}, expecting {0}");

            var schemaIdBytes = BitConverter.IsLittleEndian
                ? new[] {dataArray[4], dataArray[3], dataArray[2], dataArray[1]}
                : new [] {dataArray[1], dataArray[2], dataArray[3], dataArray[4]};

            var schemaId = BitConverter.ToInt32(schemaIdBytes, 0);

            if (_deserializerCache.ContainsKey(schemaId) == false)
            {
                var schema = await _schemaRegistryClient.GetSchemaAsync(schemaId).ConfigureAwait(false);
                var schemaJson = JObject.Parse(schema.SchemaString);
                var typeString = $"{schemaJson.SelectToken("namespace").Value<string>()}.{schemaJson.SelectToken("name").Value<string>()}";
                var recordType = Type.GetType(typeString);

                if (recordType == null)
                {
                    throw new InvalidDataException($"Deserialization failure, type {typeString} cannot be found");
                }

                var deserializerType = typeof(AvroDeserializer<>).MakeGenericType(recordType);

                dynamic deserializer = Activator.CreateInstance(deserializerType, _schemaRegistryClient, _config);

                _deserializerCache.Add(schemaId, deserializer);
            }

            return (ISpecificRecord)await _deserializerCache[schemaId].DeserializeAsync(data, false, context);
        }
    }
}
