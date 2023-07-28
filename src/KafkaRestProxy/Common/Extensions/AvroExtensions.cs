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

    private static void AddEnum(XContainer parent, GenericEnum value, string name = null,
        string nspace = null)
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

        var element = new XElement(XArray);

        if (name != null)
            element.SetAttributeValue(XName, name);

        foreach (var item in value) AddValue(element, item, arraySchema.ItemSchema);

        parent.Add(element);
    }

    private static void AddMap(XContainer parent, IDictionary<string, object> value, Schema schema,
        string name = null)
    {
        var mapSchema = GetEffectiveSchema<MapSchema>(schema);
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

    private static void AddFixed(XContainer parent, GenericFixed value, string name = null,
        string nspace = null)
    {
        var element = new XElement(XFixed);

        if (name != null)
            element.SetAttributeValue(XName, name);

        if (nspace != null)
            element.SetAttributeValue(XNamespace, nspace);

        //element.Value = BitConverter.ToString(value.Value).Replace("-", string.Empty).ToLowerInvariant();
        element.Value = Convert.ToBase64String(value.Value);

        parent.Add(element);
    }

    private static void AddDecimal(XContainer parent, AvroDecimal value, string name = null,
        string nspace = null)
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

    private static void AddUuid(XContainer parent, Guid value, string name = null,
        string nspace = null)
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
            BytesString => Convert.FromBase64String(element.Value),
            StringString => element.Value,
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

    private static GenericRecord ParseRecord(XContainer element, Schema schema)
    {
        var effectiveSchema = GetEffectiveSchema<RecordSchema>(schema);
        var record = new GenericRecord(effectiveSchema);

        foreach (var childElement in element.Elements())
        {
            var fieldName = childElement.Attribute(XName)?.Value;
            if (string.IsNullOrWhiteSpace(fieldName))
                throw new SerializationException("Element doesn't have a name.");

            if (!effectiveSchema.TryGetField(fieldName, out var field))
                if (!effectiveSchema.TryGetFieldAlias(fieldName, out field))
                    throw new SerializationException(
                        $"Field '{fieldName}' not found in schema '{effectiveSchema.SchemaName}'.");

            try
            {
                record.Add(field.Pos, ParseValue(childElement, field.Schema));
            }
            catch (Exception e)
            {
                throw new SerializationException($"Cannot parse record field '{fieldName}'.", e);
            }
        }

        return record;
    }

    private static GenericEnum ParseEnum(XElement element, Schema schema)
    {
        var effectiveSchema = GetEffectiveSchema<EnumSchema>(schema);
        return new GenericEnum(effectiveSchema, element.Value);
    }

    private static Array ParseArray(XContainer element, Schema schema)
    {
        var effectiveSchema = GetEffectiveSchema<ArraySchema>(schema);
        var list = new ArrayList();

        foreach (var childElement in element.Elements())
            list.Add(ParseValue(childElement, effectiveSchema.ItemSchema));

        return list.ToArray();
    }

    private static IDictionary<string, object> ParseMap(XElement element, Schema schema)
    {
        var effectiveSchema = GetEffectiveSchema<MapSchema>(schema);
        var map = new Dictionary<string, object>();

        foreach (var childElement in element.Elements())
        {
            var keyAttribute = childElement.Attribute(XKey);
            if (keyAttribute == null)
                throw new SerializationException("Map item element doesn't have a 'key' attribute.");

            map.Add(keyAttribute.Value,
                ParseValue(childElement.Elements().First(), effectiveSchema.ValueSchema));
        }

        return map;
    }

    private static GenericFixed ParseFixed(XElement element, Schema schema)
    {
        var effectiveSchema = GetEffectiveSchema<FixedSchema>(schema);
        //return new GenericFixed(effectiveSchema, Convert.FromHexString(element.Value));
        return new GenericFixed(effectiveSchema, Convert.FromBase64String(element.Value));
    }

    private static AvroDecimal ParseDecimal(XElement element, Schema schema)
    {
        var effectiveSchema = GetEffectiveSchema<LogicalSchema>(schema);
        var targetScale = GetScalePropertyValueFromSchema(effectiveSchema);

        var value = XmlConvert.ToDecimal(element.Value);

        var actualScale = value.GetScale();
        if (targetScale > actualScale)
            value = decimal.Multiply(value, XScales[targetScale - actualScale]);

        return new AvroDecimal(value);
    }

    private static int GetScalePropertyValueFromSchema(Schema schema, int defaultVal = 0)
    {
        var scaleVal = schema.GetProperty("scale");

        return string.IsNullOrEmpty(scaleVal) ? defaultVal : int.Parse(scaleVal, CultureInfo.CurrentCulture);
    }

    private static Guid ParseUuid(XElement element, Schema schema)
    {
        var unused = GetEffectiveSchema<LogicalSchema>(schema);
        return Guid.Parse(element.Value);
    }

    private static DateTime ParseDate(XElement element, Schema schema)
    {
        var unused = GetEffectiveSchema<LogicalSchema>(schema);
        var value = XmlConvert.ToDateTime(element.Value, XmlDateTimeSerializationMode.Unspecified);
        return value;
    }

    private static DateTime ParseTimestampMillis(XElement element, Schema schema)
    {
        var unused = GetEffectiveSchema<LogicalSchema>(schema);
        var value = XmlConvert.ToDateTime(element.Value, XmlDateTimeSerializationMode.Utc);
        return value;
    }

    private static DateTime ParseTimestampMicros(XElement element, Schema schema)
    {
        var unused = GetEffectiveSchema<LogicalSchema>(schema);
        var value = XmlConvert.ToDateTime(element.Value, XmlDateTimeSerializationMode.Utc);
        return value;
    }

    private static TimeSpan ParseTimeMillis(XElement element, Schema schema)
    {
        var unused = GetEffectiveSchema<LogicalSchema>(schema);
        var value = XmlConvert.ToTimeSpan(element.Value);
        return value;
    }

    private static TimeSpan ParseTimeMicros(XElement element, Schema schema)
    {
        var unused = GetEffectiveSchema<LogicalSchema>(schema);
        var value = XmlConvert.ToTimeSpan(element.Value);
        return value;
    }

    private static T GetEffectiveSchema<T>(Schema schema) where T : Schema
    {
        switch (schema)
        {
            case T matchedSchema:
                return matchedSchema;
            case UnionSchema unionSchema:
            {
                T result = null;

                foreach (var innerSchema in unionSchema.Schemas)
                    if (innerSchema is T matchedSchema)
                    {
                        if (result != null)
                            throw new SerializationException(
                                $"Schema '{typeof(T).Name}' presented in union more than once.");
                        result = matchedSchema;
                    }

                if (result == null)
                    throw new SerializationException($"Schema '{typeof(T).Name}' not included in union.");

                return result;
            }
            default:
                throw new SerializationException(
                    $"Cannot convert schema '{schema.GetType()}' to '{typeof(T)}'.");
        }
    }

    #endregion
}