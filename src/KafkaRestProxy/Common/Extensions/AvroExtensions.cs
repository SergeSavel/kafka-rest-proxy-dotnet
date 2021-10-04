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
using System.Collections.Generic;
using System.Xml;
using System.Xml.Linq;
using Avro;
using Avro.Generic;

namespace SergeSavel.KafkaRestProxy.Common.Extensions
{
    public static class AvroExtensions
    {
        private static readonly XName NameNamespace = "namespace";
        private static readonly XName NameName = "name";

        private static readonly XName NameNull = "null";
        private static readonly XName NameBoolean = "boolean";
        private static readonly XName NameInt = "int";
        private static readonly XName NameLong = "long";
        private static readonly XName NameFloat = "float";
        private static readonly XName NameDouble = "double";
        private static readonly XName NameBytes = "bytes";
        private static readonly XName NameString = "string";

        private static readonly XName NameRecord = "record";
        private static readonly XName NameEnum = "enum";
        private static readonly XName NameArray = "array";
        private static readonly XName NameMap = "map";
        private static readonly XName NameFixed = "fixed";

        private static readonly XName NameDecimal = "decimal";
        private static readonly XName NameUuid = "uuid";
        private static readonly XName NameDate = "date";
        private static readonly XName NameTimestampMillis = "timestamp-millis";
        private static readonly XName NameTimestampMicros = "timestamp-micros";

        private static readonly XName NameMapItem = "mapItem";
        private static readonly XName NameKey = "key";

        private static readonly DateTime StartDateUtc = new(1970, 01, 01, 0, 0, 0, DateTimeKind.Utc);

        public static string AsXmlString(this GenericRecord record)
        {
            if (record == null) return null;

            var document = new XDocument();

            AddRecord(document, record, record.Schema, record.Schema.Name, record.Schema.Namespace);

            //var result = document.ToString(SaveOptions.DisableFormatting);
            var result = document.ToString();

            return result;
        }

        private static void AddValue(XContainer parent, object value, Schema schema, string name = null)
        {
            if (value == null)
            {
                AddNull(parent, name);
                return;
            }

            switch (schema)
            {
                case PrimitiveSchema:
                    AddPrimitive(parent, value, name);
                    break;
                case LogicalSchema logicalSchema:
                    AddLogical(parent, value, logicalSchema, name);
                    break;
                case RecordSchema recordSchema when value is GenericRecord recordValue:
                    AddRecord(parent, recordValue, recordSchema, name);
                    break;
                case EnumSchema enumSchema when value is GenericEnum enumValue:
                    AddEnum(parent, enumValue, enumSchema, name);
                    break;
                case ArraySchema arraySchema when value is Array arrayValue:
                    AddArray(parent, arrayValue, arraySchema, name);
                    break;
                case MapSchema mapSchema when value is IDictionary<string, object> mapValue:
                    AddMap(parent, mapValue, mapSchema, name);
                    break;
                case FixedSchema fixedSchema when value is GenericFixed fixedValue:
                    AddFixed(parent, fixedValue, fixedSchema, name);
                    break;
                default:
                    throw new InvalidOperationException(
                        $"Unexpected combination of schema type ({schema.GetType()}) and value type ({value.GetType()}).");
            }
        }

        private static void AddPrimitive(XContainer parent, object value, string name = null,
            string nspace = null)
        {
            switch (value)
            {
                case bool boolValue:
                    AddBoolean(parent, boolValue, name, nspace);
                    break;
                case int intValue:
                    AddInt(parent, intValue, name, nspace);
                    break;
                case long longValue:
                    AddLong(parent, longValue, name, nspace);
                    break;
                case float floatValue:
                    AddFloat(parent, floatValue, name, nspace);
                    break;
                case double doubleValue:
                    AddDouble(parent, doubleValue, name, nspace);
                    break;
                case byte[] bytesValue:
                    AddBytes(parent, bytesValue, name, nspace);
                    break;
                case string stringValue:
                    AddString(parent, stringValue, name, nspace);
                    break;
                default:
                    throw new InvalidOperationException(
                        $"Unexpected primitive value type ({value.GetType()}).");
            }

            // switch (schema.Name)
            // {
            //     case "boolean" when value is bool boolValue:
            //         AddBoolean(parent, boolValue, name, nspace);
            //         break;
            //     case "int" when value is int intValue:
            //         AddInt(parent, intValue, name, nspace);
            //         break;
            //     case "long" when value is long longValue:
            //         AddLong(parent, longValue, name, nspace);
            //         break;
            //     case "float" when value is float floatValue:
            //         AddFloat(parent, floatValue, name, nspace);
            //         break;
            //     case "double" when value is double doubleValue:
            //         AddDouble(parent, floatValue, name, nspace);
            //         break;
            //     case "bytes" when value is byte[] bytesValue:
            //         AddBytes(parent, floatValue, name, nspace);
            //         break;
            //     case "string" when value is string stringValue:
            //         AddString(parent, stringValue, name, nspace);
            //         break;
            //     default:
            //         throw new InvalidOperationException(
            //             $"Unexpected combination of primitive schema type ({schema.Name}) and value type ({value.GetType()}).");
            // }
        }

        private static void AddLogical(XContainer parent, object value, LogicalSchema schema, string name = null,
            string nspace = null)
        {
            switch (schema.LogicalTypeName)
            {
                case "decimal" when value is AvroDecimal avroDecimalValue:
                    AddDecimal(parent, avroDecimalValue, name, nspace);
                    break;
                case "uuid" when value is Guid guidValue:
                    AddUuid(parent, guidValue, name, nspace);
                    break;
                case "date" when value is DateTime dateValue:
                    AddDate(parent, dateValue, name, nspace);
                    break;
                case "timestamp-millis" when value is DateTime datetimeMillisValue:
                    AddTimestampMillis(parent, datetimeMillisValue, name, nspace);
                    break;
                case "timestamp-micros" when value is DateTime datetimeMicrosValue:
                    AddTimestampMicros(parent, datetimeMicrosValue, name, nspace);
                    break;
                default:
                    throw new InvalidOperationException(
                        $"Unexpected combination of primitive schema type ({schema.Name}) and value type ({value.GetType()}).");
            }
        }

        private static void AddNull(XContainer parent, string name = null, string nspace = null)
        {
            var element = new XElement(NameNull);

            if (name != null)
                element.SetAttributeValue(NameName, name);

            if (nspace != null)
                element.SetAttributeValue(NameNamespace, nspace);

            parent.Add(element);
        }

        private static void AddBoolean(XContainer parent, bool value, string name = null, string nspace = null)
        {
            var element = new XElement(NameBoolean);

            if (name != null)
                element.SetAttributeValue(NameName, name);

            if (nspace != null)
                element.SetAttributeValue(NameNamespace, nspace);

            element.Value = XmlConvert.ToString(value);

            parent.Add(element);
        }

        private static void AddInt(XContainer parent, int value, string name = null, string nspace = null)
        {
            var element = new XElement(NameInt);

            if (name != null)
                element.SetAttributeValue(NameName, name);

            if (nspace != null)
                element.SetAttributeValue(NameNamespace, nspace);

            element.Value = XmlConvert.ToString(value);

            parent.Add(element);
        }

        private static void AddLong(XContainer parent, long value, string name = null, string nspace = null)
        {
            var element = new XElement(NameLong);

            if (name != null)
                element.SetAttributeValue(NameName, name);

            if (nspace != null)
                element.SetAttributeValue(NameNamespace, nspace);

            element.Value = XmlConvert.ToString(value);

            parent.Add(element);
        }

        private static void AddFloat(XContainer parent, float value, string name = null, string nspace = null)
        {
            var element = new XElement(NameFloat);

            if (name != null)
                element.SetAttributeValue(NameName, name);

            if (nspace != null)
                element.SetAttributeValue(NameNamespace, nspace);

            element.Value = XmlConvert.ToString(value);

            parent.Add(element);
        }

        private static void AddDouble(XContainer parent, double value, string name = null, string nspace = null)
        {
            var element = new XElement(NameDouble);

            if (name != null)
                element.SetAttributeValue(NameName, name);

            if (nspace != null)
                element.SetAttributeValue(NameNamespace, nspace);

            element.Value = XmlConvert.ToString(value);

            parent.Add(element);
        }

        private static void AddBytes(XContainer parent, byte[] value, string name = null, string nspace = null)
        {
            var element = new XElement(NameBytes);

            if (name != null)
                element.SetAttributeValue(NameName, name);

            if (nspace != null)
                element.SetAttributeValue(NameNamespace, nspace);

            element.Value = Convert.ToBase64String(value);

            parent.Add(element);
        }

        private static void AddString(XContainer parent, string value, string name = null, string nspace = null)
        {
            var element = new XElement(NameString);

            if (name != null)
                element.SetAttributeValue(NameName, name);

            if (nspace != null)
                element.SetAttributeValue(NameNamespace, nspace);

            element.Value = value;

            parent.Add(element);
        }

        private static void AddRecord(XContainer parent, GenericRecord value, RecordSchema schema, string name = null,
            string nspace = null)
        {
            var element = new XElement(NameRecord);

            if (name != null)
                element.SetAttributeValue(NameName, name);

            if (nspace != null)
                element.SetAttributeValue(NameNamespace, nspace);

            foreach (var field in schema.Fields)
                if (value.TryGetValue(field.Pos, out var fieldValue))
                    AddValue(element, fieldValue, field.Schema, field.Name);

            parent.Add(element);
        }

        private static void AddArray(XContainer parent, Array value, ArraySchema schema, string name = null)
        {
            var element = new XElement(NameArray);

            if (name != null)
                element.SetAttributeValue(NameName, name);

            foreach (var item in value) AddValue(element, item, schema.ItemSchema);

            parent.Add(element);
        }

        private static void AddMap(XContainer parent, IDictionary<string, object> value, MapSchema schema,
            string name = null)
        {
            var element = new XElement(NameMap);

            if (name != null)
                element.SetAttributeValue(NameName, name);

            foreach (var (itemKey, itemValue) in value)
            {
                var itemElement = new XElement(NameMapItem);
                itemElement.SetAttributeValue(NameKey, itemKey);
                AddValue(itemElement, itemValue, schema.ValueSchema);
                element.Add(itemElement);
            }

            parent.Add(element);
        }

        private static void AddEnum(XContainer parent, GenericEnum value, EnumSchema schema, string name = null,
            string nspace = null)
        {
            var element = new XElement(NameEnum);

            if (name != null)
                element.SetAttributeValue(NameName, name);

            if (nspace != null)
                element.SetAttributeValue(NameNamespace, nspace);

            element.Value = value.Value;

            parent.Add(element);
        }

        private static void AddFixed(XContainer parent, GenericFixed value, FixedSchema schema, string name = null,
            string nspace = null)
        {
            var element = new XElement(NameFixed);

            if (name != null)
                element.SetAttributeValue(NameName, name);

            if (nspace != null)
                element.SetAttributeValue(NameNamespace, nspace);

            element.Value = BitConverter.ToString(value.Value).Replace("-", string.Empty).ToLowerInvariant();

            parent.Add(element);
        }

        private static void AddDecimal(XContainer parent, AvroDecimal value, string name = null,
            string nspace = null)
        {
            var element = new XElement(NameDecimal);

            if (name != null)
                element.SetAttributeValue(NameName, name);

            if (nspace != null)
                element.SetAttributeValue(NameNamespace, nspace);

            var decimalValue = value.ToType<decimal>();
            element.Value = XmlConvert.ToString(decimalValue);

            parent.Add(element);
        }

        private static void AddUuid(XContainer parent, Guid value, string name = null,
            string nspace = null)
        {
            var element = new XElement(NameUuid);

            if (name != null)
                element.SetAttributeValue(NameName, name);

            if (nspace != null)
                element.SetAttributeValue(NameNamespace, nspace);

            element.Value = XmlConvert.ToString(value);

            parent.Add(element);
        }

        private static void AddDate(XContainer parent, DateTime value, string name = null,
            string nspace = null)
        {
            var element = new XElement(NameDate);

            if (name != null)
                element.SetAttributeValue(NameName, name);

            if (nspace != null)
                element.SetAttributeValue(NameNamespace, nspace);

            var intValue = (int)(value - StartDateUtc).TotalDays;
            element.Value = XmlConvert.ToString(intValue);

            parent.Add(element);
        }

        private static void AddTimestampMillis(XContainer parent, DateTime value, string name = null,
            string nspace = null)
        {
            var element = new XElement(NameTimestampMillis);

            if (name != null)
                element.SetAttributeValue(NameName, name);

            if (nspace != null)
                element.SetAttributeValue(NameNamespace, nspace);

            var longValue = (long)(value - StartDateUtc).TotalMilliseconds;
            element.Value = XmlConvert.ToString(longValue);

            parent.Add(element);
        }

        private static void AddTimestampMicros(XContainer parent, DateTime value, string name = null,
            string nspace = null)
        {
            var element = new XElement(NameTimestampMicros);

            if (name != null)
                element.SetAttributeValue(NameName, name);

            if (nspace != null)
                element.SetAttributeValue(NameNamespace, nspace);

            var longValue = (long)((value - StartDateUtc).TotalMilliseconds * 1000);
            element.Value = XmlConvert.ToString(longValue);

            parent.Add(element);
        }
    }
}