import yaml
import os
from .extension_function import ExtensionFunction


class ExtensionRegistry:
    def __init__(self, urls) -> None:
        self.registry = {}
        self.num_functions = 0
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
            self.registry[url]["scalar_functions"] = {}
            for i, f in enumerate(content["scalar_functions"]):
                if f["name"] in self.registry[url]["scalar_functions"]:
                    self.registry[url]["scalar_functions"][f["name"]].append(
                        ExtensionFunction(f, self.num_functions + i)
                    )
                else:
                    self.registry[url]["scalar_functions"][f["name"]] = [
                        ExtensionFunction(f, self.num_functions + i)
                    ]

            self.num_functions += len(content["scalar_functions"])

        if "aggregate_functions" in content:
            self.registry[url]["aggregate_functions"] = {}
            for i, f in enumerate(content["aggregate_functions"]):
                if f["name"] in self.registry[url]["aggregate_functions"]:
                    self.registry[url]["aggregate_functions"][f["name"]].append(
                        ExtensionFunction(f, self.num_functions + i)
                    )
                else:
                    self.registry[url]["aggregate_functions"][f["name"]] = [
                        ExtensionFunction(f, self.num_functions + i)
                    ]

            self.num_functions += len(content["aggregate_functions"])

    def lookup_scalar_function(self, url: str, name: str) -> ExtensionFunction:
        return self.registry[url]["scalar_functions"][name]

    def lookup_aggregate_function(self, url: str, name: str) -> ExtensionFunction:
        return self.registry[url]["aggregate_functions"][name]
