import re
from substrait.gen.proto.type_pb2 import Type


def to_substrait_type(dtype: str):
    if dtype in ("bool", "boolean"):
        return Type(bool=Type.Boolean())
    elif dtype == "i8":
        return Type(i8=Type.I8())
    elif dtype == "i16":
        return Type(i16=Type.I16())
    elif dtype == "i32":
        return Type(i32=Type.I32())
    elif dtype == "i64":
        return Type(i64=Type.I64())
    elif dtype == "fp32":
        return Type(fp32=Type.FP32())
    elif dtype == "fp64":
        return Type(fp64=Type.FP64())
    elif dtype == "timestamp":
        return Type(timestamp=Type.Timestamp())
    elif dtype == "timestamp_tz":
        return Type(timestamp_tz=Type.TimestampTZ())
    elif dtype == "date":
        return Type(date=Type.Date())
    elif dtype == "time":
        return Type(time=Type.Time())
    elif dtype == "interval_year":
        return Type(interval_year=Type.IntervalYear())
    elif dtype.startswith("decimal") or dtype.startswith("DECIMAL"):
        (_, scale, precision, _) = re.split(r"\W+", dtype)

        return Type(decimal=Type.Decimal(scale=int(scale), precision=int(precision)))
    else:
        raise Exception(f"Unknown type - {dtype}")
