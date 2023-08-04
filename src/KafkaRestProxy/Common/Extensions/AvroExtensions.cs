// Copyright 2023 Sergey Savelev
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Collections;
using System.Collections.Concurrent;
using System.Globalization;
using System.Xml;
using System.Xml.Linq;
using Avro;
using Avro.Generic;
using SergeSavel.KafkaRestProxy.Common.Exceptions;

namespace SergeSavel.KafkaRestProxy.Common.Extensions;

public static class AvroExtensions
{
    private const string NamespaceString = "namespace";
    private const string NameString = "name";
    private const string NullString = "null";
    private const string BooleanString = "boolean";
    private const string IntString = "int";
    private const string LongString = "long";
    private const string FloatString = "float";
    private const string DoubleString = "double";
    private const string BytesString = "bytes";
    private const string StringString = "string";
    private const string RecordString = "record";
    private const string EnumString = "enum";
    private const string ArrayString = "array";
    private const string MapString = "map";
    private const string FixedString = "fixed";
    private const string DecimalString = "decimal";
    private const string UuidString = "uuid";
    private const string DateString = "date";
    private const string TimeMillisString = "time-millis";
    private const string TimeMicrosString = "time-micros";
    private const string TimestampMillisString = "timestamp-millis";
    private const string TimestampMicrosString = "timestamp-micros";
    private const string MapItemString = "mapItem";
    private const string KeyString = "key";

    private static readonly XName XNamespace = NamespaceString;
    private static readonly XName XName = NameString;
    private static readonly XName XNull = NullString;
    private static readonly XName XBoolean = BooleanString;
    private static readonly XName XInt = IntString;
    private static readonly XName XLong = LongString;
    private static readonly XName XFloat = FloatString;
    private static readonly XName XDouble = DoubleString;
    private static readonly XName XBytes = BytesString;
    private static readonly XName XString = StringString;
    private static readonly XName XRecord = RecordString;
    private static readonly XName XEnum = EnumString;
    private static readonly XName XArray = ArrayString;
    private static readonly XName XMap = MapString;
    private static readonly XName XFixed = FixedString;
    private static readonly XName XDecimal = DecimalString;
    private static readonly XName XUuid = UuidString;

    //private static readonly XName XDate = DateString;
    //private static readonly XName XTimeMillis = TimeMillisString;
    //private static readonly XName XTimeMicros = TimeMicrosString;
    //private static readonly XName XTimestampMillis = TimestampMillisString;
    //private static readonly XName XTimestampMicros = TimestampMicrosString;
    private static readonly XName XMapItem = MapItemString;
    private static readonly XName XKey = KeyString;

    private static readonly decimal[] XScales =
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

    #region Deserialization

    public static string AsXmlString(this GenericRecord record)
    {
        if (record == null) return null;

        var document = new XDocument();

        AddRecord(document, record, record.Schema.Name, record.Schema.Namespace);

        var result = document.ToString(SaveOptions.DisableFormatting);

        return result;
    }

    private static void AddValue(XContainer parent, object value, Schema schema, string name = null)
    {
        if (value == null)
        {
            AddNull(parent, name);
            return;
        }

        if (value is bool booleanValue)
            AddBoolean(parent, booleanValue, name);
        else if (value is int intValue)
            AddInt(parent, intValue, name);
        else if (value is long longValue)
            AddLong(parent, longValue, name);
        else if (value is float floatValue)
            AddFloat(parent, floatValue, name);
        else if (value is double doubleValue)
            AddDouble(parent, doubleValue, name);
        else if (value is byte[] bytesValue)
            AddBytes(parent, bytesValue, name);
        else if (value is string stringValue)
            AddString(parent, stringValue, name);
        else if (value is GenericRecord recordValue)
            AddRecord(parent, recordValue, name);
        else if (value is GenericEnum enumValue)
            AddEnum(parent, enumValue, name);
        else if (value is Array arrayValue)
            AddArray(parent, arrayValue, schema, name);
        else if (value is IDictionary<string, object> mapValue)
            AddMap(parent, mapValue, schema, name);
        else if (value is GenericFixed fixedValue)
            AddFixed(parent, fixedValue, name);
        else if (value is AvroDecimal decimalValue)
            AddDecimal(parent, decimalValue, name);
        else if (value is Guid guidValue)
            AddUuid(parent, guidValue, name);
        else if (value is DateTime datetimeValue)
            AddDateTime(parent, datetimeValue, schema, name);
        else if (value is TimeSpan timespanValue)
            AddTimeSpan(parent, timespanValue, schema, name);
    }

    private static void AddNull(XContainer parent, string name = null, string nspace = null)
    {
        var element = new XElement(XNull);

        if (name != null)
            element.SetAttributeValue(XName, name);

        if (nspace != null)
            element.SetAttributeValue(XNamespace, nspace);

        parent.Add(element);
    }

    private static void AddBoolean(XContainer parent, bool value, string name = null, string nspace = null)
    {
        var element = new XElement(XBoolean);

        if (name != null)
            element.SetAttributeValue(XName, name);

        if (nspace != null)
            element.SetAttributeValue(XNamespace, nspace);

        element.Value = XmlConvert.ToString(value);

        parent.Add(element);
    }

    private static void AddInt(XContainer parent, int value, string name = null, string nspace = null)
    {
        var element = new XElement(XInt);

        if (name != null)
            element.SetAttributeValue(XName, name);

        if (nspace != null)
            element.SetAttributeValue(XNamespace, nspace);

        element.Value = XmlConvert.ToString(value);

        parent.Add(element);
    }

    private static void AddLong(XContainer parent, long value, string name = null, string nspace = null)
    {
        var element = new XElement(XLong);

        if (name != null)
            element.SetAttributeValue(XName, name);

        if (nspace != null)
            element.SetAttributeValue(XNamespace, nspace);

        element.Value = XmlConvert.ToString(value);

        parent.Add(element);
    }

    private static void AddFloat(XContainer parent, float value, string name = null, string nspace = null)
    {
        var element = new XElement(XFloat);

        if (name != null)
            element.SetAttributeValue(XName, name);

        if (nspace != null)
            element.SetAttributeValue(XNamespace, nspace);

        element.Value = XmlConvert.ToString(value);

        parent.Add(element);
    }

    private static void AddDouble(XContainer parent, double value, string name = null, string nspace = null)
    {
        var element = new XElement(XDouble);

        if (name != null)
            element.SetAttributeValue(XName, name);

        if (nspace != null)
            element.SetAttributeValue(XNamespace, nspace);

        element.Value = XmlConvert.ToString(value);

        parent.Add(element);
    }

    private static void AddBytes(XContainer parent, byte[] value, string name = null, string nspace = null)
    {
        var element = new XElement(XBytes);

        if (name != null)
            element.SetAttributeValue(XName, name);

        if (nspace != null)
            element.SetAttributeValue(XNamespace, nspace);

        element.Value = Convert.ToBase64String(value);

        parent.Add(element);
    }

    private static void AddString(XContainer parent, string value, string name = null, string nspace = null)
    {
        var element = new XElement(XString);

        if (name != null)
            element.SetAttributeValue(XName, name);

        if (nspace != null)
            element.SetAttributeValue(XNamespace, nspace);

        element.Value = value;

        parent.Add(element);
    }

    private static void AddRecord(XContainer parent, GenericRecord value, string name = null,
        string nspace = null)
    {
        var element = new XElement(XRecord);

        if (name != null)
            element.SetAttributeValue(XName, name);

        if (nspace != null)
            element.SetAttributeValue(XNamespace, nspace);

        foreach (var field in value.Schema.Fields)
            if (value.TryGetValue(field.Pos, out var fieldValue))
                AddValue(element, fieldValue, field.Schema, field.Name);

        parent.Add(element);
    }

    private static void AddEnum(XContainer parent, GenericEnum value, string name = null, string nspace = null)
    {
        var element = new XElement(XEnum);

        if (name != null)
            element.SetAttributeValue(XName, name);

        if (nspace != null)
            element.SetAttributeValue(XNamespace, nspace);

        element.Value = value.Value;

        parent.Add(element);
    }

    private static void AddArray(XContainer parent, Array value, Schema schema, string name = null)
    {
        var arraySchema = GetEffectiveSchema<ArraySchema>(schema);
        if (arraySchema == null)
            throw new DeserializationException("Array schema not found.");

        var element = new XElement(XArray);

        if (name != null)
            element.SetAttributeValue(XName, name);

        foreach (var item in value) AddValue(element, item, arraySchema.ItemSchema);

        parent.Add(element);
    }

    private static void AddMap(XContainer parent, IDictionary<string, object> value, Schema schema, string name = null)
    {
        var mapSchema = GetEffectiveSchema<MapSchema>(schema);
        if (mapSchema == null)
            throw new DeserializationException("Array schema not found.");

        var element = new XElement(XMap);

        if (name != null)
            element.SetAttributeValue(XName, name);

        foreach (var (itemKey, itemValue) in value)
        {
            var itemElement = new XElement(XMapItem);
            itemElement.SetAttributeValue(XKey, itemKey);
            AddValue(itemElement, itemValue, mapSchema.ValueSchema);
            element.Add(itemElement);
        }

        parent.Add(element);
    }

    private static void AddFixed(XContainer parent, GenericFixed value, string name = null, string nspace = null)
    {
        var element = new XElement(XFixed);

        if (name != null)
            element.SetAttributeValue(XName, name);

        if (nspace != null)
            element.SetAttributeValue(XNamespace, nspace);

        element.Value = Convert.ToBase64String(value.Value);

        parent.Add(element);
    }

    private static void AddDecimal(XContainer parent, AvroDecimal value, string name = null, string nspace = null)
    {
        var element = new XElement(XDecimal);

        if (name != null)
            element.SetAttributeValue(XName, name);

        if (nspace != null)
            element.SetAttributeValue(XNamespace, nspace);

        var decimalValue = value.ToType<decimal>();
        element.Value = XmlConvert.ToString(decimalValue);

        parent.Add(element);
    }

    private static void AddUuid(XContainer parent, Guid value, string name = null, string nspace = null)
    {
        var element = new XElement(XUuid);

        if (name != null)
            element.SetAttributeValue(XName, name);

        if (nspace != null)
            element.SetAttributeValue(XNamespace, nspace);

        element.Value = XmlConvert.ToString(value);

        parent.Add(element);
    }

    private static void AddDateTime(XContainer parent, DateTime value, Schema schema, string name = null,
        string nspace = null)
    {
        var logicalSchema = GetEffectiveSchema<LogicalSchema>(schema);
        if (logicalSchema == null)
            throw new DeserializationException("Logical schema not found.");

        var element = new XElement(logicalSchema.LogicalTypeName); // ???

        if (name != null)
            element.SetAttributeValue(XName, name);

        if (nspace != null)
            element.SetAttributeValue(XNamespace, nspace);

        element.Value = XmlConvert.ToString(value, XmlDateTimeSerializationMode.Unspecified);

        parent.Add(element);
    }

    private static void AddTimeSpan(XContainer parent, TimeSpan value, Schema schema, string name = null,
        string nspace = null)
    {
        var logicalSchema = GetEffectiveSchema<LogicalSchema>(schema);
        if (logicalSchema == null)
            throw new DeserializationException("Logical schema not found.");

        var element = new XElement(logicalSchema.LogicalTypeName); // ???

        if (name != null)
            element.SetAttributeValue(XName, name);

        if (nspace != null)
            element.SetAttributeValue(XNamespace, nspace);

        element.Value = XmlConvert.ToString(value);

        parent.Add(element);
    }

    #endregion

    #region Serialization

    public static GenericRecord AsAvroGenericRecord(this string xmlString,
        ConcurrentDictionary<string, RecordSchema> schemaCache)
    {
        if (string.IsNullOrEmpty(xmlString))
            return null;

        var doc = XDocument.Parse(xmlString);

        var rootElement = doc.Root;
        if (rootElement == null)
            throw new SerializationException("XML document doesn't have a root element.");

        if (rootElement.Name.LocalName != RecordString)
            throw new SerializationException("Invalid root element name.");

        RecordSchema schema;
        try
        {
            schema = GetSchema(rootElement, schemaCache);
        }
        catch (Exception e)
        {
            throw new SerializationException("Cannot parse record schema.", e);
        }

        GenericRecord result;
        try
        {
            result = ParseRecord(rootElement, schema);
        }
        catch (Exception e)
        {
            throw new SerializationException("Cannot parse record.", e);
        }

        return result;
    }

    private static RecordSchema GetSchema(XElement rootElement, ConcurrentDictionary<string, RecordSchema> schemaCache)
    {
        var schemaName = rootElement.Attribute(XName)?.Value;
        if (string.IsNullOrWhiteSpace(schemaName))
            throw new SerializationException("Record name not provided.");

        var schemaNamespace = rootElement.Attribute(XNamespace)?.Value ?? string.Empty;

        var schemaFullName =
            (string.IsNullOrWhiteSpace(schemaNamespace) ? schemaName : schemaNamespace + "." + schemaName)
            .ToUpperInvariant();

        if (!schemaCache.TryGetValue(schemaFullName, out var schema))
            throw new SerializationException("Schema isn't found in local cache.");

        return schema;
    }

    private static object ParseValue(XElement element, Schema schema)
    {
        var elementName = element.Name.LocalName;

        return elementName switch
        {
            NullString => null,
            BooleanString => XmlConvert.ToBoolean(element.Value),
            IntString => XmlConvert.ToInt32(element.Value),
            LongString => XmlConvert.ToInt64(element.Value),
            FloatString => XmlConvert.ToSingle(element.Value),
            DoubleString => XmlConvert.ToDouble(element.Value),
            BytesString => ParseBytes(element, schema),
            StringString => ParseString(element, schema),
            RecordString => ParseRecord(element, schema),
            EnumString => ParseEnum(element, schema),
            ArrayString => ParseArray(element, schema),
            MapString => ParseMap(element, schema),
            FixedString => ParseFixed(element, schema),
            DecimalString => ParseDecimal(element, schema),
            UuidString => ParseUuid(element, schema),
            DateString => ParseDate(element, schema),
            TimeMillisString => ParseTimeMillis(element, schema),
            TimeMicrosString => ParseTimeMicros(element, schema),
            TimestampMillisString => ParseTimestampMillis(element, schema),
            TimestampMicrosString => ParseTimestampMicros(element, schema),
            _ => throw new SerializationException($"Invalid element: '{elementName}'.")
        };
    }

    private static object ParseBytes(XElement element, Schema schema)
    {
        var bytesSchema = GetEffectiveSchema<PrimitiveSchema>(schema, Schema.Type.Bytes);
        if (bytesSchema != null) return Convert.FromBase64String(element.Value);

        var fixedSchema = GetEffectiveSchema<FixedSchema>(schema, Schema.Type.Fixed);
        if (fixedSchema != null) return new GenericFixed(fixedSchema, Convert.FromBase64String(element.Value));

        throw new SerializationException(
            $"Cannot infer bytes schema from schema '{schema}'.");
    }

    private static object ParseString(XElement element, Schema schema)
    {
        var stringSchema = GetEffectiveSchema<PrimitiveSchema>(schema, Schema.Type.String);
        if (stringSchema != null) return element.Value;

        var enumSchema = GetEffectiveSchema<EnumSchema>(schema, Schema.Type.Enumeration);
        if (enumSchema != null) return new GenericEnum(enumSchema, element.Value);

        throw new SerializationException(
            $"Cannot infer string schema from schema '{schema}'.");
    }

    private static GenericRecord ParseRecord(XContainer element, Schema schema)
    {
        var recordSchema = GetEffectiveSchema<RecordSchema>(schema, Schema.Type.Record);
        if (recordSchema != null)
        {
            var record = new GenericRecord(recordSchema);

            foreach (var childElement in element.Elements())
            {
                var fieldName = childElement.Attribute(XName)?.Value;
                if (string.IsNullOrWhiteSpace(fieldName))
                    throw new SerializationException("Element doesn't have a name.");

                if (!recordSchema.TryGetField(fieldName, out var field))
                    if (!recordSchema.TryGetFieldAlias(fieldName, out field))
                        throw new SerializationException(
                            $"Field '{fieldName}' not found in schema '{recordSchema.SchemaName}'.");

                try
                {
                    var value = ParseValue(childElement, field.Schema);
                    record.Add(field.Pos, value);
                }
                catch (Exception e)
                {
                    throw new SerializationException($"Cannot parse record field '{fieldName}'.", e);
                }
            }

            return record;
        }

        throw new SerializationException(
            $"Cannot infer record schema from schema '{schema}'.");
    }

    private static GenericEnum ParseEnum(XElement element, Schema schema)
    {
        var enumSchema = GetEffectiveSchema<EnumSchema>(schema, Schema.Type.Enumeration);
        if (enumSchema != null) return new GenericEnum(enumSchema, element.Value);

        throw new SerializationException(
            $"Cannot infer enum schema from schema '{schema}'.");
    }

    private static Array ParseArray(XContainer element, Schema schema)
    {
        var arraySchema = GetEffectiveSchema<ArraySchema>(schema, Schema.Type.Array);
        if (arraySchema != null)
        {
            var list = new ArrayList();
            foreach (var childElement in element.Elements())
                list.Add(ParseValue(childElement, arraySchema.ItemSchema));
            return list.ToArray();
        }

        throw new SerializationException(
            $"Cannot infer array schema from schema '{schema}'.");
    }

    private static IDictionary<string, object> ParseMap(XContainer element, Schema schema)
    {
        var mapSchema = GetEffectiveSchema<MapSchema>(schema, Schema.Type.Map);
        if (mapSchema != null)
        {
            var map = new Dictionary<string, object>();
            foreach (var childElement in element.Elements())
            {
                var keyAttribute = childElement.Attribute(XKey);
                if (keyAttribute == null)
                    throw new SerializationException("Map item element doesn't have a 'key' attribute.");

                map.Add(keyAttribute.Value,
                    ParseValue(childElement.Elements().First(), mapSchema.ValueSchema));
            }

            return map;
        }

        throw new SerializationException(
            $"Cannot infer map schema from schema '{schema}'.");
    }

    private static GenericFixed ParseFixed(XElement element, Schema schema)
    {
        var fixedSchema = GetEffectiveSchema<FixedSchema>(schema, Schema.Type.Fixed);
        if (fixedSchema != null) return new GenericFixed(fixedSchema, Convert.FromBase64String(element.Value));

        throw new SerializationException(
            $"Cannot infer fixed schema from schema '{schema}'.");
    }

    private static object ParseDecimal(XElement element, Schema schema)
    {
        var decimalSchema = GetLogicalSchema(schema, DecimalString);
        if (decimalSchema != null)
        {
            var targetScale = GetScalePropertyValueFromSchema(decimalSchema);

            var decimalValue = XmlConvert.ToDecimal(element.Value);

            var actualScale = decimalValue.GetScale();
            if (targetScale > actualScale)
                decimalValue = decimal.Multiply(decimalValue, XScales[targetScale - actualScale]);

            return new AvroDecimal(decimalValue);
        }

        var longSchema = GetEffectiveSchema<PrimitiveSchema>(schema, Schema.Type.Long);
        if (longSchema != null) return XmlConvert.ToInt64(element.Value);

        var intSchema = GetEffectiveSchema<PrimitiveSchema>(schema, Schema.Type.Int);
        if (intSchema != null) return XmlConvert.ToInt32(element.Value);

        throw new SerializationException(
            $"Cannot infer logical schema ('{DecimalString}') from schema '{schema}'.");
    }

    private static int GetScalePropertyValueFromSchema(LogicalSchema schema, int defaultVal = 0)
    {
        var scaleVal = schema.GetProperty("scale");

        return string.IsNullOrEmpty(scaleVal) ? defaultVal : int.Parse(scaleVal, CultureInfo.CurrentCulture);
    }

    private static Guid ParseUuid(XElement element, Schema schema)
    {
        var uuidSchema = GetLogicalSchema(schema, UuidString);
        if (uuidSchema != null) return Guid.Parse(element.Value);

        throw new SerializationException(
            $"Cannot infer logical schema ('{UuidString}') from schema '{schema}'.");
    }

    private static DateTime ParseDate(XElement element, Schema schema)
    {
        var dateSchema = GetLogicalSchema(schema, DateString);
        if (dateSchema != null)
        {
            var value = XmlConvert.ToDateTime(element.Value, XmlDateTimeSerializationMode.Unspecified);
            return value;
        }

        var timestampMillisSchema = GetLogicalSchema(schema, TimestampMillisString);
        if (timestampMillisSchema != null)
        {
            var value = XmlConvert.ToDateTime(element.Value, XmlDateTimeSerializationMode.Utc);
            return value;
        }

        var timestampMicrosSchema = GetLogicalSchema(schema, TimestampMicrosString);
        if (timestampMicrosSchema != null)
        {
            var value = XmlConvert.ToDateTime(element.Value, XmlDateTimeSerializationMode.Utc);
            return value;
        }

        throw new SerializationException(
            $"Cannot infer logical schema ('{DateString}') from schema '{schema}'.");
    }

    private static DateTime ParseTimestampMillis(XElement element, Schema schema)
    {
        var timestampMillisSchema = GetLogicalSchema(schema, TimestampMillisString);
        if (timestampMillisSchema != null)
        {
            var value = XmlConvert.ToDateTime(element.Value, XmlDateTimeSerializationMode.Utc);
            return value;
        }

        var timestampMicrosSchema = GetLogicalSchema(schema, TimestampMicrosString);
        if (timestampMicrosSchema != null)
        {
            var value = XmlConvert.ToDateTime(element.Value, XmlDateTimeSerializationMode.Utc);
            return value;
        }

        var dateSchema = GetLogicalSchema(schema, DateString);
        if (dateSchema != null)
        {
            var value = XmlConvert.ToDateTime(element.Value, XmlDateTimeSerializationMode.Unspecified);
            if (value is { Hour: 0, Minute: 0, Second: 0, Millisecond: 0 })
                return value;
        }

        throw new SerializationException(
            $"Cannot infer logical schema ('{TimestampMillisString}') from schema '{schema}'.");
    }

    private static DateTime ParseTimestampMicros(XElement element, Schema schema)
    {
        var timestampMicrosSchema = GetLogicalSchema(schema, TimestampMicrosString);
        if (timestampMicrosSchema != null)
        {
            var value = XmlConvert.ToDateTime(element.Value, XmlDateTimeSerializationMode.Utc);
            return value;
        }

        throw new SerializationException(
            $"Cannot infer logical schema ('{TimestampMicrosString}') from schema '{schema}'.");
    }

    private static TimeSpan ParseTimeMillis(XElement element, Schema schema)
    {
        var timeMillisSchema = GetLogicalSchema(schema, TimeMillisString);
        if (timeMillisSchema != null)
        {
            var value = XmlConvert.ToTimeSpan(element.Value);
            return value;
        }

        var timeMicrosSchema = GetLogicalSchema(schema, TimeMicrosString);
        if (timeMicrosSchema != null)
        {
            var value = XmlConvert.ToTimeSpan(element.Value);
            return value;
        }

        throw new SerializationException(
            $"Cannot infer logical schema ('{TimeMillisString}') from schema '{schema}'.");
    }

    private static TimeSpan ParseTimeMicros(XElement element, Schema schema)
    {
        var timeMicrosSchema = GetLogicalSchema(schema, TimeMicrosString);
        if (timeMicrosSchema != null)
        {
            var value = XmlConvert.ToTimeSpan(element.Value);
            return value;
        }

        throw new SerializationException(
            $"Cannot infer logical schema ('{TimeMicrosString}') from schema '{schema}'.");
    }

    private static T GetEffectiveSchema<T>(Schema schema) where T : Schema
    {
        switch (schema)
        {
            case T typedSchema:
                return typedSchema;
            case UnionSchema unionSchema:
            {
                T result = null;
                foreach (var innerSchema in unionSchema.Schemas)
                    if (innerSchema is T typedSchema)
                    {
                        if (result != null)
                            throw new SerializationException(
                                $"Schema '{typeof(T)}' is presented in union more than once.");
                        result = typedSchema;
                    }

                return result;
            }
            default:
                return null;
        }
    }

    private static T GetEffectiveSchema<T>(Schema schema, Schema.Type tag) where T : Schema
    {
        switch (schema)
        {
            case T typedSchema:
                return typedSchema.Tag == tag ? typedSchema : null;
            case UnionSchema unionSchema:
            {
                T result = null;
                foreach (var innerSchema in unionSchema.Schemas)
                    if (innerSchema is T typedSchema && tag == typedSchema.Tag)
                    {
                        if (result != null)
                            throw new SerializationException(
                                $"Schema '{typeof(T)}({tag})' is presented in union more than once.");
                        result = typedSchema;
                    }

                return result;
            }
            default:
                return null;
        }
    }

    private static LogicalSchema GetLogicalSchema(Schema schema, string logicalTypeName)
    {
        switch (schema)
        {
            case LogicalSchema logicalSchema:
                return logicalSchema.LogicalTypeName.Equals(logicalTypeName) ? logicalSchema : null;
            case UnionSchema unionSchema:
            {
                LogicalSchema result = null;
                foreach (var innerSchema in unionSchema.Schemas)
                    if (innerSchema is LogicalSchema logicalSchema &&
                        logicalSchema.LogicalTypeName.Equals(logicalTypeName))
                    {
                        if (result != null)
                            throw new SerializationException(
                                $"Schema '{typeof(LogicalSchema)}({logicalTypeName})' is presented in union more than once.");
                        result = logicalSchema;
                    }

                return result;
            }
            default:
                return null;
        }
    }

    #endregion
}