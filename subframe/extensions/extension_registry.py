import yaml
import os
from .extension_scalar_function import ExtensionScalarFunction


class ExtensionRegistry:
    def __init__(self, urls) -> None:
        self.registry = {}
        self.num_scalar_functions = 0
        for url in urls:
            self.load_url(url)

    def load_url(self, url):
        resource_url = url.replace(
            "https://github.com/substrait-io/substrait/blob/main/", ""
        )

        resource_url = os.path.join(os.path.dirname(__file__), resource_url)
        with open(resource_url, "r") as f:
            body = f.read()
        self.registry[url] = {}

        content = yaml.load(body, Loader=yaml.FullLoader)

        if "scalar_functions" in content:
            self.registry[url]["scalar_functions"] = {
                f["name"]: ExtensionScalarFunction(f, self.num_scalar_functions + i)
                for i, f in enumerate(content["scalar_functions"])
            }

            self.num_scalar_functions += len(content["scalar_functions"])

    def lookup_scalar_function(self, url: str, name: str) -> ExtensionScalarFunction:
        return self.registry[url]["scalar_functions"][name]
