// Copyright 2021 Sergey Savelev
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Xml.Linq;
using Avro;
using Avro.Generic;
using SergeSavel.KafkaRestProxy.Common.Extensions;

namespace SergeSavel.KafkaRestProxy.Producer
{
    public static class ProducerGenericMapper
    {
        private static readonly XName Namespace = "namespace";
        private static readonly XName Name = "name";
        private static readonly XName Key = "key";

        [Obsolete] private static readonly ulong[] Powers =
        {
            1,
            10,
            100,
            1000,
            10000,
            100000,
            1000000,
            10000000,
            100000000,
            1000000000,
            10000000000,
            100000000000,
            1000000000000,
            10000000000000,
            100000000000000,
            1000000000000000,
            10000000000000000,
            100000000000000000,
            1000000000000000000,
            10000000000000000000
        };

        private static readonly decimal[] Scales =
        {
            1m,
            1.0m,
            1.00m,
            1.000m,
            1.0000m,
            1.00000m,
            1.000000m,
            1.0000000m,
            1.00000000m,
            1.000000000m,
            1.0000000000m,
            1.00000000000m,
            1.000000000000m,
            1.0000000000000m,
            1.00000000000000m,
            1.000000000000000m,
            1.0000000000000000m,
            1.00000000000000000m,
            1.000000000000000000m,
            1.0000000000000000000m,
            1.00000000000000000000m,
            1.000000000000000000000m,
            1.0000000000000000000000m,
            1.00000000000000000000000m,
            1.000000000000000000000000m,
            1.0000000000000000000000000m,
            1.00000000000000000000000000m,
            1.000000000000000000000000000m,
            1.0000000000000000000000000000m,
            1.00000000000000000000000000000m,
            1.000000000000000000000000000000m,
            1.0000000000000000000000000000000m,
            1.00000000000000000000000000000000m,
            1.000000000000000000000000000000000m,
            1.0000000000000000000000000000000000m,
            1.00000000000000000000000000000000000m,
            1.000000000000000000000000000000000000m,
            1.0000000000000000000000000000000000000m,
            1.00000000000000000000000000000000000000m,
            1.000000000000000000000000000000000000000m,
            1.0000000000000000000000000000000000000000m,
            1.00000000000000000000000000000000000000000m,
            1.000000000000000000000000000000000000000000m,
            1.0000000000000000000000000000000000000000000m,
            1.00000000000000000000000000000000000000000000m,
            1.000000000000000000000000000000000000000000000m,
            1.0000000000000000000000000000000000000000000000m,
            1.00000000000000000000000000000000000000000000000m,
            1.000000000000000000000000000000000000000000000000m,
            1.0000000000000000000000000000000000000000000000000m,
            1.00000000000000000000000000000000000000000000000000m
        };

        public static GenericRecord GetGenericRecord(string source, string schemaString,
            ConcurrentDictionary<string, RecordSchema> schemaCache)
        {
            if (string.IsNullOrEmpty(source))
                return null;

            var doc = XDocument.Parse(source);

            var rootElement = doc.Root;
            if (rootElement == null)
                throw new InvalidOperationException("XML document doesn't have a root element.");

            if (rootElement.Name.LocalName != "record")
                throw new InvalidOperationException("Invalid root element name.");

            var schema = GetSchema(rootElement, schemaString, schemaCache);

            var result = ParseRecord(rootElement, schema);

            return result;
        }

        private static RecordSchema GetSchema(XElement rootElement, string schemaString,
            ConcurrentDictionary<string, RecordSchema> schemaCache)
        {
            var schemaName = rootElement.Attribute(Name)?.Value;
            if (string.IsNullOrWhiteSpace(schemaName))
                throw new InvalidOperationException("Record name not provided..");

            var schemaNamespace = rootElement.Attribute(Namespace)?.Value ?? string.Empty;

            var schemaFullName =
                (string.IsNullOrWhiteSpace(schemaNamespace) ? schemaName : schemaNamespace + "." + schemaName)
                .ToUpperInvariant();

            RecordSchema schema;

            if (schemaString == null)
            {
                if (!schemaCache.TryGetValue(schemaFullName, out schema))
                    throw new InvalidOperationException("Schema isn't found in local cache.");
            }
            else
            {
                try
                {
                    schema = (RecordSchema)Schema.Parse(schemaString);
                }
                catch (SchemaParseException e)
                {
                    throw new InvalidOperationException("An error occured while parsing schema: " + e.Message);
                }

                schemaCache.AddOrUpdate(schemaFullName, _ => schema, (_, _) => schema);
            }

            return schema;
        }

        private static object ParseValue(XElement element, Schema schema)
        {
            var elementName = element.Name.LocalName;

            return elementName switch
            {
                "null" => null,
                "boolean" => bool.Parse(element.Value),
                "int" => int.Parse(element.Value),
                "long" => long.Parse(element.Value),
                "float" => float.Parse(element.Value),
                "double" => double.Parse(element.Value),
                "bytes" => Convert.FromBase64String(element.Value),
                "string" => element.Value,
                "record" => ParseRecord(element, schema),
                "enum" => ParseEnum(element, schema),
                "array" => ParseArray(element, schema),
                "map" => ParseMap(element, schema),
                "fixed" => ParseFixed(element, schema),
                "decimal" => ParseDecimal(element, schema),
                "uuid" => ParseUuid(element, schema),
                "date" => ParseDate(element, schema),
                "time-millis" => ParseTimeMillis(element, schema),
                "time-micros" => ParseTimeMicros(element, schema),
                "timestamp-millis" => ParseTimestampMillis(element, schema),
                "timestamp-micros" => ParseTimestampMicros(element, schema),
                _ => throw new InvalidOperationException($"Invalid element: '{elementName}'.")
            };
        }

        private static GenericRecord ParseRecord(XElement element, Schema schema)
        {
            var effectiveSchema = GetEffectiveSchema<RecordSchema>(schema);
            var record = new GenericRecord(effectiveSchema);

            foreach (var childElement in element.Elements())
            {
                var fieldName = childElement.Attribute(Name)?.Value;
                if (string.IsNullOrWhiteSpace(fieldName))
                    throw new InvalidOperationException("Element doesn't have a name.");

                if (!effectiveSchema.TryGetField(fieldName, out var field))
                    if (!effectiveSchema.TryGetFieldAlias(fieldName, out field))
                        throw new InvalidOperationException(
                            $"Field '{fieldName}' not found in schema '{effectiveSchema.SchemaName}'.");

                record.Add(field.Pos, ParseValue(childElement, field.Schema));
            }

            return record;
        }

        private static GenericEnum ParseEnum(XElement element, Schema schema)
        {
            var effectiveSchema = GetEffectiveSchema<EnumSchema>(schema);
            return new GenericEnum(effectiveSchema, element.Value);
        }

        private static Array ParseArray(XElement element, Schema schema)
        {
            var effectiveSchema = GetEffectiveSchema<ArraySchema>(schema);
            var array = new ArrayList();

            foreach (var childElement in element.Elements())
                array.Add(ParseValue(childElement, effectiveSchema.ItemSchema));

            return array.ToArray();
        }

        private static IDictionary<string, object> ParseMap(XElement element, Schema schema)
        {
            var effectiveSchema = GetEffectiveSchema<MapSchema>(schema);
            var map = new Dictionary<string, object>();

            foreach (var childElement in element.Elements())
            {
                var keyAttribute = childElement.Attribute(Key);
                if (keyAttribute == null)
                    throw new InvalidOperationException("Map item element doesn't have a 'key' attribute.");

                map.Add(keyAttribute.Value,
                    ParseValue(childElement.Elements().First(), effectiveSchema.ValueSchema));
            }

            return map;
        }

        private static GenericFixed ParseFixed(XElement element, Schema schema)
        {
            var effectiveSchema = GetEffectiveSchema<FixedSchema>(schema);
            return new GenericFixed(effectiveSchema, Convert.FromHexString(element.Value));
        }

        private static AvroDecimal ParseDecimal(XElement element, Schema schema)
        {
            var effectiveSchema = GetEffectiveSchema<LogicalSchema>(schema);
            var targetScale = GetScalePropertyValueFromSchema(effectiveSchema);

            var decimalValue = decimal.Parse(element.Value, CultureInfo.InvariantCulture.NumberFormat);
            var actualScale = decimalValue.GetScale();

            if (targetScale > actualScale)
                // decimalValue *= Powers[targetScale-actualScale];
                // var longValue = decimal.ToUInt64(decimalValue);
                // var bigint = BigInteger.Multiply(Powers[targetScale], longValue);
                //     result = new AvroDecimal(bigint, targetScale);
                decimalValue = decimal.Multiply(decimalValue, Scales[targetScale - actualScale]);

            return new AvroDecimal(decimalValue);
        }

        private static int GetScalePropertyValueFromSchema(Schema schema, int defaultVal = 0)
        {
            var scaleVal = schema.GetProperty("scale");

            return string.IsNullOrEmpty(scaleVal) ? defaultVal : int.Parse(scaleVal, CultureInfo.CurrentCulture);
        }

        private static string ParseUuid(XElement element, Schema schema)
        {
            var unused = GetEffectiveSchema<LogicalSchema>(schema);
            return element.Value;
        }

        private static DateTime ParseDate(XElement element, Schema schema)
        {
            var unused = GetEffectiveSchema<LogicalSchema>(schema);
            return DateTime.Parse(element.Value, CultureInfo.InvariantCulture.DateTimeFormat);
        }

        private static TimeSpan ParseTimeMillis(XElement element, Schema schema)
        {
            var unused = GetEffectiveSchema<LogicalSchema>(schema);
            return TimeSpan.Parse(element.Value, CultureInfo.InvariantCulture.DateTimeFormat);
        }

        private static TimeSpan ParseTimeMicros(XElement element, Schema schema)
        {
            var unused = GetEffectiveSchema<LogicalSchema>(schema);
            return TimeSpan.Parse(element.Value, CultureInfo.InvariantCulture.DateTimeFormat);
        }

        private static DateTime ParseTimestampMillis(XElement element, Schema schema)
        {
            var unused = GetEffectiveSchema<LogicalSchema>(schema);
            return DateTime.Parse(element.Value, CultureInfo.InvariantCulture.DateTimeFormat);
        }

        private static DateTime ParseTimestampMicros(XElement element, Schema schema)
        {
            var unused = GetEffectiveSchema<LogicalSchema>(schema);
            return DateTime.Parse(element.Value, CultureInfo.InvariantCulture.DateTimeFormat);
        }

        private static T GetEffectiveSchema<T>(Schema schema) where T : Schema
        {
            switch (schema)
            {
                case T matchedSchema:
                    return matchedSchema;
                case UnionSchema unionSchema:
                {
                    foreach (var innerSchema in unionSchema.Schemas)
                        if (innerSchema is T matchedSchema)
                            return matchedSchema;
                    throw new InvalidOperationException($"'{typeof(T).Name}' not included in union.");
                }
                default:
                    throw new InvalidOperationException($"Cannot convert '{schema.GetType()}' to '{typeof(T)}'.");
            }
        }
    }
}