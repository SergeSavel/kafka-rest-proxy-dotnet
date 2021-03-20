﻿using Confluent.Kafka;
using Error = pro.savel.KafkaRestProxy.Common.Contract.Error;

namespace pro.savel.KafkaRestProxy.Common.Mappers
{
    public static class CommonMapper
    {
        public static Error Map(Confluent.Kafka.Error source)
        {
            if (source.Code == ErrorCode.NoError)
                return null;

            return new Error()
            {
                Code = (int) source.Code,
                Reason = source.Reason
            };
        }
    }
}