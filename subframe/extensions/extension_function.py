class ExtensionFunction:
    def __init__(self, function_definition: dict, anchor: int):
        self.function_definition = function_definition
        self.anchor = anchor

    def lookup_signature(self, args: list[str]):
        for impl in self.function_definition["impls"]:
            impl_args = impl["args"]
            if len(impl_args) == len(args) and args == [x["value"] for x in impl_args]:
                return (
                    self.anchor,
                    self.function_definition["name"]
                    + ":"
                    + "_".join([x["value"] for x in impl_args]),
                    impl,
                )
