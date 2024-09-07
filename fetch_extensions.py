import os
from urllib.request import urlopen

urls = [
    "https://github.com/substrait-io/substrait/blob/main/extensions/functions_aggregate_approx.yaml",
    "https://github.com/substrait-io/substrait/blob/main/extensions/functions_aggregate_decimal_output.yaml",
    "https://github.com/substrait-io/substrait/blob/main/extensions/functions_aggregate_generic.yaml",
    "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml",
    "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic_decimal.yaml",
    "https://github.com/substrait-io/substrait/blob/main/extensions/functions_boolean.yaml",
    "https://github.com/substrait-io/substrait/blob/main/extensions/functions_comparison.yaml",
    "https://github.com/substrait-io/substrait/blob/main/extensions/functions_datetime.yaml",
    "https://github.com/substrait-io/substrait/blob/main/extensions/functions_geometry.yaml",
    "https://github.com/substrait-io/substrait/blob/main/extensions/functions_logarithmic.yaml",
    "https://github.com/substrait-io/substrait/blob/main/extensions/functions_rounding.yaml",
    "https://github.com/substrait-io/substrait/blob/main/extensions/functions_set.yaml",
    "https://github.com/substrait-io/substrait/blob/main/extensions/functions_string.yaml",
    "https://github.com/substrait-io/substrait/blob/main/extensions/type_variations.yaml",
]

for url in urls:
    resource_url = url.replace(
        "https://github.com/substrait-io/substrait/blob",
        "https://raw.githubusercontent.com/substrait-io/substrait",
    )
    with urlopen(resource_url) as response:
        body = response.read()

    file_name = resource_url.split("/")[-1]
    path = os.path.join("subframe", "extensions", "extensions", file_name)

    print(file_name)

    with open(path, "wb") as f:
        f.write(body)
