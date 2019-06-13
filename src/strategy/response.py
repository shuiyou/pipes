# To use this code, make sure you
#
#     import json
#
# and then, to convert JSON from a string, do
#
#     result = response_from_dict(json.loads(json_string)
#    StragetyOne决策引擎的Response

from dataclasses import dataclass
from typing import Optional, Any, List, TypeVar, Type, cast, Callable
from datetime import datetime
import dateutil.parser

T = TypeVar("T")


def from_datetime(x: Any) -> datetime:
    return dateutil.parser.parse(x)


def from_str(x: Any) -> str:
    assert isinstance(x, str)
    return x


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


def from_int(x: Any) -> int:
    assert isinstance(x, int) and not isinstance(x, bool)
    return x


def from_float(x: Any) -> float:
    assert isinstance(x, (float, int)) and not isinstance(x, bool)
    return float(x)


def is_type(t: Type[T], x: Any) -> T:
    assert isinstance(x, t)
    return x


def to_float(x: Any) -> float:
    assert isinstance(x, float)
    return x


def from_list(f: Callable[[Any], T], x: Any) -> List[T]:
    assert isinstance(x, list)
    return [f(y) for y in x]


@dataclass
class ReasonVariables:
    out_decision_branch_code: Optional[str]
    out_reject_reason_code: Optional[str]
    out_risk_reason_code: Optional[str]

    @staticmethod
    def from_dict(obj: Any) -> 'ReasonVariables':
        assert isinstance(obj, dict)
        out_decision_branch_code = from_union([from_str, from_none], obj.get("out_decisionBranchCode"))
        out_reject_reason_code = from_union([from_str, from_none], obj.get("out_rejectReasonCode"))
        out_risk_reason_code = from_union([from_str, from_none], obj.get("out_riskReasonCode"))
        return ReasonVariables(out_decision_branch_code, out_reject_reason_code, out_risk_reason_code)

    def to_dict(self) -> dict:
        result: dict = {}
        result["out_decisionBranchCode"] = from_union([from_str, from_none], self.out_decision_branch_code)
        result["out_rejectReasonCode"] = from_union([from_str, from_none], self.out_reject_reason_code)
        result["out_riskReasonCode"] = from_union([from_str, from_none], self.out_risk_reason_code)
        return result


@dataclass
class Reason:
    variables: Optional[ReasonVariables]

    @staticmethod
    def from_dict(obj: Any) -> 'Reason':
        assert isinstance(obj, dict)
        variables = from_union([ReasonVariables.from_dict, from_none], obj.get("Variables"))
        return Reason(variables)

    def to_dict(self) -> dict:
        result: dict = {}
        result["Variables"] = from_union([lambda x: to_class(ReasonVariables, x), from_none], self.variables)
        return result


@dataclass
class Category:
    reason: Optional[Reason]

    @staticmethod
    def from_dict(obj: Any) -> 'Category':
        assert isinstance(obj, dict)
        reason = from_union([Reason.from_dict, from_none], obj.get("Reason"))
        return Category(reason)

    def to_dict(self) -> dict:
        result: dict = {}
        result["Reason"] = from_union([lambda x: to_class(Reason, x), from_none], self.reason)
        return result


@dataclass
class ApplicationVariables:
    out_is_query: Optional[str]
    out_strategy_branch: Optional[int]
    out_result: Optional[str]
    td_risk_tel_hit_high_att: Optional[int]
    risk_score_level1: Optional[int]
    temp_hd_score: Optional[float]
    temp_hf_score: Optional[int]
    temp_jxl_score: Optional[float]
    temp_qh_score: Optional[int]
    temp_td_score: Optional[float]
    temp_hf_other_max_level: Optional[int]
    temp_model_score: Optional[int]
    temp_risk_native: Optional[int]
    temp_hf_pub_info_max_level: Optional[int]
    temp_hf_tax_arrears_max_level: Optional[int]

    @staticmethod
    def from_dict(obj: Any) -> 'ApplicationVariables':
        assert isinstance(obj, dict)
        out_is_query = from_union([from_str, from_none], obj.get("out_isQuery"))
        out_strategy_branch = from_union([from_none, lambda x: int(from_str(x))], obj.get("out_strategyBranch"))
        out_result = from_union([from_str, from_none], obj.get("out_result"))
        td_risk_tel_hit_high_att = from_union([from_int, from_none], obj.get("td_risk_tel_hit_high_att"))
        risk_score_level1 = from_union([from_int, from_none], obj.get("risk_score_level1"))
        temp_hd_score = from_union([from_float, from_none], obj.get("temp_hd_score"))
        temp_hf_score = from_union([from_int, from_none], obj.get("temp_hf_score"))
        temp_jxl_score = from_union([from_float, from_none], obj.get("temp_jxl_score"))
        temp_qh_score = from_union([from_int, from_none], obj.get("temp_qh_score"))
        temp_td_score = from_union([from_float, from_none], obj.get("temp_td_score"))
        temp_hf_other_max_level = from_union([from_int, from_none], obj.get("temp_hf_other_max_level"))
        temp_model_score = from_union([from_int, from_none], obj.get("temp_model_score"))
        temp_risk_native = from_union([from_int, from_none], obj.get("temp_risk_native"))
        temp_hf_pub_info_max_level = from_union([from_int, from_none], obj.get("temp_hf_pub_info_max_level"))
        temp_hf_tax_arrears_max_level = from_union([from_int, from_none], obj.get("temp_hf_tax_arrears_max_level"))
        return ApplicationVariables(out_is_query, out_strategy_branch, out_result, td_risk_tel_hit_high_att,
                                    risk_score_level1, temp_hd_score, temp_hf_score, temp_jxl_score, temp_qh_score,
                                    temp_td_score, temp_hf_other_max_level, temp_model_score, temp_risk_native,
                                    temp_hf_pub_info_max_level, temp_hf_tax_arrears_max_level)

    def to_dict(self) -> dict:
        result: dict = {}
        result["out_isQuery"] = from_union([from_str, from_none], self.out_is_query)
        result["out_strategyBranch"] = from_union([lambda x: from_none((lambda x: is_type(type(None), x))(x)),
                                                   lambda x: from_str(
                                                       (lambda x: str((lambda x: is_type(int, x))(x)))(x))],
                                                  self.out_strategy_branch)
        result["out_result"] = from_union([from_str, from_none], self.out_result)
        result["td_risk_tel_hit_high_att"] = from_union([from_int, from_none], self.td_risk_tel_hit_high_att)
        result["risk_score_level1"] = from_union([from_int, from_none], self.risk_score_level1)
        result["temp_hd_score"] = from_union([to_float, from_none], self.temp_hd_score)
        result["temp_hf_score"] = from_union([from_int, from_none], self.temp_hf_score)
        result["temp_jxl_score"] = from_union([to_float, from_none], self.temp_jxl_score)
        result["temp_qh_score"] = from_union([from_int, from_none], self.temp_qh_score)
        result["temp_td_score"] = from_union([to_float, from_none], self.temp_td_score)
        result["temp_hf_other_max_level"] = from_union([from_int, from_none], self.temp_hf_other_max_level)
        result["temp_model_score"] = from_union([from_int, from_none], self.temp_model_score)
        result["temp_risk_native"] = from_union([from_int, from_none], self.temp_risk_native)
        result["temp_hf_pub_info_max_level"] = from_union([from_int, from_none], self.temp_hf_pub_info_max_level)
        result["temp_hf_tax_arrears_max_level"] = from_union([from_int, from_none], self.temp_hf_tax_arrears_max_level)
        return result


@dataclass
class Application:
    variables: Optional[ApplicationVariables]
    categories: Optional[List[Category]]

    @staticmethod
    def from_dict(obj: Any) -> 'Application':
        assert isinstance(obj, dict)
        variables = from_union([ApplicationVariables.from_dict, from_none], obj.get("Variables"))
        categories = from_union([lambda x: from_list(Category.from_dict, x), from_none], obj.get("Categories"))
        return Application(variables, categories)

    def to_dict(self) -> dict:
        result: dict = {}
        result["Variables"] = from_union([lambda x: to_class(ApplicationVariables, x), from_none], self.variables)
        result["Categories"] = from_union([lambda x: from_list(lambda x: to_class(Category, x), x), from_none],
                                          self.categories)
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
    organization_code: Optional[str]
    process_version: Optional[int]
    layout_version: Optional[int]

    @staticmethod
    def from_dict(obj: Any) -> 'Header':
        assert isinstance(obj, dict)
        inquiry_code = from_union([from_str, from_none], obj.get("InquiryCode"))
        process_code = from_union([from_str, from_none], obj.get("ProcessCode"))
        organization_code = from_union([from_str, from_none], obj.get("OrganizationCode"))
        process_version = from_union([from_int, from_none], obj.get("ProcessVersion"))
        layout_version = from_union([from_int, from_none], obj.get("LayoutVersion"))
        return Header(inquiry_code, process_code, organization_code, process_version, layout_version)

    def to_dict(self) -> dict:
        result: dict = {}
        result["InquiryCode"] = from_union([from_str, from_none], self.inquiry_code)
        result["ProcessCode"] = from_union([from_str, from_none], self.process_code)
        result["OrganizationCode"] = from_union([from_str, from_none], self.organization_code)
        result["ProcessVersion"] = from_union([from_int, from_none], self.process_version)
        result["LayoutVersion"] = from_union([from_int, from_none], self.layout_version)
        return result


@dataclass
class Error:
    inquiry_date: Optional[datetime]
    code: Optional[int]
    description: Optional[str]
    engine_version: Optional[str]
    engine_stack_trace: Optional[str]

    @staticmethod
    def from_dict(obj: Any) -> 'Error':
        assert isinstance(obj, dict)
        inquiry_date = from_union([from_datetime, from_none], obj.get("InquiryDate"))
        code = from_union([from_none, lambda x: int(from_str(x))], obj.get("Code"))
        description = from_union([from_str, from_none], obj.get("Description"))
        engine_version = from_union([from_str, from_none], obj.get("EngineVersion"))
        engine_stack_trace = from_union([from_str, from_none], obj.get("EngineStackTrace"))
        return Error(inquiry_date, code, description, engine_version, engine_stack_trace)

    def to_dict(self) -> dict:
        result: dict = {}
        result["InquiryDate"] = from_union([lambda x: x.isoformat(), from_none], self.inquiry_date)
        result["Code"] = from_union([lambda x: from_none((lambda x: is_type(type(None), x))(x)),
                                     lambda x: from_str((lambda x: str((lambda x: is_type(int, x))(x)))(x))], self.code)
        result["Description"] = from_union([from_str, from_none], self.description)
        result["EngineVersion"] = from_union([from_str, from_none], self.engine_version)
        result["EngineStackTrace"] = from_union([from_str, from_none], self.engine_stack_trace)
        return result


@dataclass
class StrategyOneResponse:
    header: Optional[Header]
    body: Optional[Body]
    error: Optional[Error]

    @staticmethod
    def from_dict(obj: Any) -> 'StrategyOneResponse':
        assert isinstance(obj, dict)
        header = from_union([Header.from_dict, from_none], obj.get("Header"))
        body = from_union([Body.from_dict, from_none], obj.get("Body"))
        return StrategyOneResponse(header, body)

    def to_dict(self) -> dict:
        result: dict = {}
        result["Header"] = from_union([lambda x: to_class(Header, x), from_none], self.header)
        result["Body"] = from_union([lambda x: to_class(Body, x), from_none], self.body)
        result["Error"] = from_union([lambda x: to_class(Error, x), from_none], self.error)
        return result


@dataclass
class Response:
    strategy_one_response: Optional[StrategyOneResponse]

    @staticmethod
    def from_dict(obj: Any) -> 'Response':
        assert isinstance(obj, dict)
        strategy_one_response = from_union([StrategyOneResponse.from_dict, from_none], obj.get("StrategyOneResponse"))
        return Response(strategy_one_response)

    def to_dict(self) -> dict:
        result: dict = {}
        result["StrategyOneResponse"] = from_union([lambda x: to_class(StrategyOneResponse, x), from_none], self.strategy_one_response)
        return result


def response_from_dict(s: Any) -> Response:
    return Response.from_dict(s)


def response_to_dict(x: Response) -> Any:
    return to_class(Response, x)
