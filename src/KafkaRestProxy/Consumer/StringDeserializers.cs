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
using System.Text;
using Confluent.Kafka;

namespace SergeSavel.KafkaRestProxy.Consumer
{
    public static class StringDeserializers
    {
        public static IDeserializer<string> Utf8 = new Utf8Deserializer();

        public static IDeserializer<string> Null = new NullDeserializer();

        public static IDeserializer<string> Ignore = new IgnoreDeserializer();

        public static IDeserializer<string> Base64 = new Base64Deserializer();

        private class Utf8Deserializer : IDeserializer<string>
        {
            public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            {
                if (isNull) return null;

                return Encoding.UTF8.GetString(data);
            }
        }

        private class NullDeserializer : IDeserializer<string>
        {
            public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            {
                if (!isNull)
                    throw new ArgumentException(
                        "Deserializer<Null> may only be used to deserialize data that is null.");

                return null;
            }
        }

        private class IgnoreDeserializer : IDeserializer<string>
        {
            public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            {
                return null;
            }
        }

        private class Base64Deserializer : IDeserializer<string>
        {
            public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            {
                if (isNull) return null;

                return Convert.ToBase64String(data);
            }
        }
    }
}