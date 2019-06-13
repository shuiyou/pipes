# To use this code, make sure you
#
#     import json
#
# and then, to convert JSON from a string, do
#
#     result = request_from_dict(json.loads(json_string))

from dataclasses import dataclass
from typing import Any, Optional, TypeVar, Type, cast


T = TypeVar("T")


def from_none(x: Any) -> Any:
    assert x is None
    return x


def from_union(fs, x):
    for f in fs:
        try:
            return f(x)
        except:
            pass
    assert False


def to_class(c: Type[T], x: Any) -> dict:
    assert isinstance(x, c)
    return cast(Any, x).to_dict()


def from_str(x: Any) -> str:
    assert isinstance(x, str)
    return x


@dataclass
class Variables:
    pass

    @staticmethod
    def from_dict(obj: Any) -> 'Variables':
        assert isinstance(obj, dict)
        return Variables()

    def to_dict(self) -> dict:
        result: dict = {}
        return result


@dataclass
class Application:
    variables: Optional[Variables]

    @staticmethod
    def from_dict(obj: Any) -> 'Application':
        assert isinstance(obj, dict)
        variables = from_union([Variables.from_dict, from_none], obj.get("Variables"))
        return Application(variables)

    def to_dict(self) -> dict:
        result: dict = {}
        result["Variables"] = from_union([lambda x: to_class(Variables, x), from_none], self.variables)
        return result


@dataclass
class Body:
    application: Optional[Application]

    @staticmethod
    def from_dict(obj: Any) -> 'Body':
        assert isinstance(obj, dict)
        application = from_union([Application.from_dict, from_none], obj.get("Application"))
        return Body(application)

    def to_dict(self) -> dict:
        result: dict = {}
        result["Application"] = from_union([lambda x: to_class(Application, x), from_none], self.application)
        return result


@dataclass
class Header:
    inquiry_code: Optional[str]
    process_code: Optional[str]

    @staticmethod
    def from_dict(obj: Any) -> 'Header':
        assert isinstance(obj, dict)
        inquiry_code = from_union([from_str, from_none], obj.get("InquiryCode"))
        process_code = from_union([from_str, from_none], obj.get("ProcessCode"))
        return Header(inquiry_code, process_code)

    def to_dict(self) -> dict:
        result: dict = {}
        result["InquiryCode"] = from_union([from_str, from_none], self.inquiry_code)
        result["ProcessCode"] = from_union([from_str, from_none], self.process_code)
        return result


@dataclass
class StrategyOneRequest:
    header: Optional[Header]
    body: Optional[Body]

    @staticmethod
    def from_dict(obj: Any) -> 'StrategyOneRequest':
        assert isinstance(obj, dict)
        header = from_union([Header.from_dict, from_none], obj.get("Header"))
        body = from_union([Body.from_dict, from_none], obj.get("Body"))
        return StrategyOneRequest(header, body)

    def to_dict(self) -> dict:
        result: dict = {}
        result["Header"] = from_union([lambda x: to_class(Header, x), from_none], self.header)
        result["Body"] = from_union([lambda x: to_class(Body, x), from_none], self.body)
        return result


@dataclass
class Request:
    strategy_one_request: Optional[StrategyOneRequest]

    @staticmethod
    def from_dict(obj: Any) -> 'Request':
        assert isinstance(obj, dict)
        strategy_one_request = from_union([StrategyOneRequest.from_dict, from_none], obj.get("StrategyOneRequest"))
        return Request(strategy_one_request)

    def to_dict(self) -> dict:
        result: dict = {}
        result["StrategyOneRequest"] = from_union([lambda x: to_class(StrategyOneRequest, x), from_none], self.strategy_one_request)
        return result


def request_from_dict(s: Any) -> Request:
    return Request.from_dict(s)


def request_to_dict(x: Request) -> Any:
    return to_class(Request, x)
