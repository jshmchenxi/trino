/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.operator.scalar.timestamp;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.type.Constraint;

import java.time.format.DateTimeFormatter;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.type.DateTimes.formatTimestamp;
import static io.prestosql.type.DateTimes.scaleEpochMillisToMicros;
import static java.time.ZoneOffset.UTC;

@ScalarFunction("to_iso8601")
public final class ToIso8601
{
    private static final DateTimeFormatter ISO8601_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss");

    // 1 digit for year sign
    // 6 digits for year -- TODO: we should constrain this further. A 6-digit year seems useless
    // 15 digits for -MM-DDTHH:MM:SS
    // min(p, 1) for the fractional second period (i.e., no period if p == 0)
    // p for the fractional digits
    private static final String RESULT_LENGTH = "1 + 6 + 15 + min(p, 1) + p";

    private ToIso8601() {}

    @LiteralParameters({"p", "n"})
    @SqlType("varchar(n)")
    @Constraint(variable = "n", expression = RESULT_LENGTH)
    public static Slice toIso8601(@LiteralParameter("p") long precision, @SqlType("timestamp(p)") long timestamp)
    {
        long epochMicros = timestamp;
        if (precision <= 3) {
            epochMicros = scaleEpochMillisToMicros(timestamp);
        }

        return utf8Slice(formatTimestamp((int) precision, epochMicros, 0, UTC, ISO8601_FORMATTER));
    }

    @LiteralParameters({"p", "n"})
    @SqlType("varchar(n)")
    @Constraint(variable = "n", expression = RESULT_LENGTH)
    public static Slice toIso8601(@LiteralParameter("p") long precision, @SqlType("timestamp(p)") LongTimestamp timestamp)
    {
        return utf8Slice(formatTimestamp((int) precision, timestamp.getEpochMicros(), timestamp.getPicosOfMicro(), UTC, ISO8601_FORMATTER));
    }
}
