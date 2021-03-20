using System.ComponentModel.DataAnnotations;

namespace pro.savel.KafkaRestProxy.Common.Contract
{
    public class Error
    {
        [Required] public int Code { get; init; }

        [Required] public string Reason { get; init; }
    }
}