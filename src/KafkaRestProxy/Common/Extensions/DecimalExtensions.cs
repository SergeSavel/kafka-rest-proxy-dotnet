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

namespace SergeSavel.KafkaRestProxy.Common.Extensions;

public static class DecimalExtensions
{
    public static int GetScale(this decimal value)
    {
        if (value == 0)
            return 0;
        var bits = decimal.GetBits(value);
        return (bits[3] >> 16) & 0x7F;
    }

    public static int GetPrecision(this decimal value)
    {
        if (value == 0)
            return 0;
        var bits = decimal.GetBits(value);
        var d = new decimal(bits[0], bits[1], bits[2], false, 0);
        return (int)Math.Floor(Math.Log10((double)d)) + 1;
    }
}