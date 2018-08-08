from homeloans.tables import Application, PreviousApplication
from homeloans.tables import Bureau, BureauBalance
from homeloans.tables import CreditCardBalance
from homeloans.tables import CashBalance
from homeloans.tables import InstalmentPayments
from homeloans.config import MISSING_VALS
from decimal import Decimal
from functools import wraps, partial
from collections import defaultdict
import csv


def handle_missing_values(missing_vals):
    def _handle_missing(f):
        @wraps(f)
        def func_wrapper(*args, **kwargs):
            if kwargs['s'] in missing_vals:
                return None
            else:
                return f(**kwargs)
        return func_wrapper
    return _handle_missing


@handle_missing_values(missing_vals=MISSING_VALS)
def convert_str(s):
    return s.strip()


@handle_missing_values(missing_vals=MISSING_VALS)
def convert_bool(s, *, true_value="Y", false_value="N"):
    if s == true_value:
        return True
    elif s == false_value:
        return False
    else:
        raise ValueError("Invalid value %s." % s)


@handle_missing_values(missing_vals=MISSING_VALS)
def convert_int(s):
    return int(s)


@handle_missing_values(missing_vals=MISSING_VALS)
def float_to_int(s):
    return int(float(s))


@handle_missing_values(missing_vals=MISSING_VALS)
def convert_numeric(s):
    return Decimal(s)


_application_field_converters = {
    "sk_id_curr": convert_int,
    "flag_own_car": convert_bool,
    "flag_own_realty": convert_bool,
    "cnt_children": convert_int,
    "amt_income_total": convert_numeric,
    "amt_credit": convert_numeric,
    "amt_annuity": convert_numeric,
    "amt_goods_price": convert_numeric,
    "region_population_relative": convert_numeric,
    "days_birth": convert_int,
    "days_employed": convert_int,
    "days_registration": float_to_int,
    "days_id_publish": convert_int,
    "own_car_age": float_to_int,
    "flag_mobil": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_work_phone": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_emp_phone": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_cont_mobile": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_phone": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_email": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "cnt_fam_members": float_to_int,
    "region_rating_client": convert_int,
    "region_rating_client_w_city": convert_int,
    "hour_appr_process_start": convert_int,
    "reg_region_not_live_region": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "reg_region_not_work_region": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "live_region_not_work_region": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "reg_city_not_live_city": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "reg_city_not_work_city": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "live_city_not_work_city": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "ext_source_1": convert_numeric,
    "ext_source_2": convert_numeric,
    "ext_source_3": convert_numeric,
    "apartments_avg": convert_numeric,
    "basementarea_avg": convert_numeric,
    "years_beginexpluatation_avg": convert_numeric,
    "years_build_avg": convert_numeric,
    "commonarea_avg": convert_numeric,
    "elevators_avg": convert_numeric,
    "entrances_avg": convert_numeric,
    "floorsmax_avg": convert_numeric,
    "floorsmin_avg": convert_numeric,
    "landarea_avg": convert_numeric,
    "livingapartments_avg": convert_numeric,
    "livingarea_avg": convert_numeric,
    "nonlivingapartments_avg": convert_numeric,
    "nonlivingarea_avg": convert_numeric,
    "apartments_mode": convert_numeric,
    "basementarea_mode": convert_numeric,
    "years_beginexpluatation_mode": convert_numeric,
    "years_build_mode": convert_numeric,
    "commonarea_mode": convert_numeric,
    "elevators_mode": convert_numeric,
    "entrances_mode": convert_numeric,
    "floorsmax_mode": convert_numeric,
    "floorsmin_mode": convert_numeric,
    "landarea_mode": convert_numeric,
    "livingapartments_mode": convert_numeric,
    "livingarea_mode": convert_numeric,
    "nonlivingapartments_mode": convert_numeric,
    "nonlivingarea_mode": convert_numeric,
    "apartments_medi": convert_numeric,
    "basementarea_medi": convert_numeric,
    "years_beginexpluatation_medi": convert_numeric,
    "years_build_medi": convert_numeric,
    "commonarea_medi": convert_numeric,
    "elevators_medi": convert_numeric,
    "entrances_medi": convert_numeric,
    "floorsmax_medi": convert_numeric,
    "floorsmin_medi": convert_numeric,
    "landarea_medi": convert_numeric,
    "livingapartments_medi": convert_numeric,
    "livingarea_medi": convert_numeric,
    "nonlivingapartments_medi": convert_numeric,
    "totalarea_mode": convert_numeric,
    "obs_30_cnt_social_circle": float_to_int,
    "def_30_cnt_social_circle": float_to_int,
    "obs_60_cnt_social_circle": float_to_int,
    "def_60_cnt_social_circle": float_to_int,
    "days_last_phone_change": float_to_int,
    "flag_document_2": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_document_3": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_document_4": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_document_5": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_document_6": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_document_7": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_document_8": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_document_9": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_document_10": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_document_11": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_document_12": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_document_13": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_document_14": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_document_15": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_document_16": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_document_17": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_document_18": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_document_19": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_document_20": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "flag_document_21": partial(convert_bool, true_value="1", false_value="0", missing_symbols=MISSING_VALS),
    "amt_req_credit_bureau_hour": float_to_int,
    "amt_req_credit_bureau_day": float_to_int,
    "amt_req_credit_bureau_week": float_to_int,
    "amt_req_credit_bureau_mon": float_to_int,
    "amt_req_credit_bureau_qtr": float_to_int,
    "amt_req_credit_bureau_year": float_to_int
}

_instalment_payments_converter = {
    "sk_id_prev": convert_int,
    "sk_id_curr": convert_int,
    "num_instalment_version": float_to_int,
    "num_instalment_number": convert_int,
    "days_instalment": float_to_int,
    "days_entry_payment": float_to_int,
    "amt_instalment": convert_numeric,
    "amt_payment": convert_numeric
}

_prev_application_converters = {
    "sk_id_prev": convert_int,
    "sk_id_curr": convert_int,
    "amt_annuity": convert_numeric,
    "amt_application": convert_numeric,
    "amt_credit": convert_numeric,
    "amt_down_payment": convert_numeric,
    "amt_goods_price": convert_numeric,
    "hour_appr_process_start": convert_int,
    "flag_last_appl_per_contract": convert_bool,
    "nflag_last_appl_in_day": convert_int,
    "rate_down_payment": convert_numeric,
    "rate_interest_primary": convert_numeric,
    "rate_interest_privileged": convert_numeric,
    "days_decision": convert_int,
    "sellerplace_area": convert_int,
    "cnt_payment": float_to_int,
    "days_first_drawing": float_to_int,
    "days_first_due": float_to_int,
    "days_last_due_1st_version": float_to_int,
    "days_last_due": float_to_int,
    "days_termination": float_to_int,
    "nflag_insured_on_approval": float_to_int
}

_bureau_converters = {
    "sk_id_curr": convert_int,
    "sk_bureau_id": convert_int,
    "days_credit": convert_int,
    "credit_day_overdue": convert_int,
    "days_credit_enddate": float_to_int,
    "days_enddate_fact": float_to_int,
    "amt_credit_max_overdue": convert_numeric,
    "cnt_credit_prolong": convert_int,
    "amt_credit_sum": convert_numeric,
    "amt_credit_sum_debt": convert_numeric,
    "amt_credit_sum_limit": convert_numeric,
    "amt_credit_sum_overdue": convert_numeric,
    "days_credit_update": convert_int,
    "amt_annuity": convert_numeric
}

_bureau_balance_converters = {
    "sk_id_bureau": convert_int,
    "months_balance": convert_int,
}

_credit_card_converters = {
    "sk_id_prev": convert_int,
    "sk_id_curr": convert_int,
    "months_balance": convert_int,
    "amt_balance": convert_numeric,
    "amt_credit_limit_actual": convert_int,
    "amt_drawings_atm_current": convert_numeric,
    "amt_drawings_current": convert_numeric,
    "amt_drawings_other_current": convert_numeric,
    "amt_drawings_pos_current": convert_numeric,
    "amt_inst_min_regularity": convert_numeric,
    "amt_payment_current": convert_numeric,
    "amt_payment_total_current": convert_numeric,
    "amt_recivable": convert_numeric,
    "amt_total_receivable": convert_numeric,
    "cnt_drawings_atm_current": float_to_int,
    "cnt_drawings_current": convert_int,
    "cnt_drawings_other_current": float_to_int,
    "cnt_instalment_mature_cum": float_to_int,
    "sk_dpd": convert_int,
    "sk_dpd_def": convert_int
}

_cash_balance_converters = {
    "sk_id_prev": convert_int,
    "sk_id_curr": convert_int,
    "months_balance": convert_int,
    "cnt_instalment": float_to_int,
    "cnt_instalment_future": float_to_int,
    "sk_dpd": convert_int,
    "sk_dpd_def": convert_int
}


def _load_data(fn, session, target_table, converter, nbatch=10000):
    objs = []
    conv = defaultdict(lambda: convert_str)
    conv.update(converter or {})

    with open(fn, "r") as fp:
        rdr = csv.reader(fp, delimiter=',', quotechar='"')
        cols = next(rdr)

        for toks in rdr:
            flds = dict([(c.lower(), conv[c.lower()](toks[i])) for i, c in enumerate(cols) if toks[i]])
            objs.append(target_table(**flds))

            if len(objs) == nbatch:
                print("inserting")
                session.bulk_save_objects(objs)
                objs = []

    if len(objs) > 0:
        session.bulk_save_objects(objs)


def load_application_train(fn, session, nbatch=10000):
    _load_data(fn, session, target_table=Application, converter=_application_field_converters, nbatch=nbatch)


def load_application_test(fn, session, nbatch=10000):
    _load_data(fn, session, target_table=Application, converter=_application_field_converters, nbatch=nbatch)


def load_bureau(fn, session, nbatch=10000):
    _load_data(fn, session, target_table=Bureau, converter=_bureau_converters, nbatch=nbatch)


def load_bureau_balance(fn, session, nbatch=10000):
    _load_data(fn, session, target_table=BureauBalance, converter=_bureau_balance_converters, nbatch=nbatch)


def load_credit_card_balance(fn, session, nbatch=10000):
    _load_data(fn, session, target_table=CreditCardBalance, converter=_credit_card_converters, nbatch=nbatch)


def load_instalment_payments(fn, session, nbatch=10000):
    _load_data(fn, session, target_table=InstalmentPayments, converter=_instalment_payments_converter, nbatch=nbatch)


def load_cash_balance(fn, session, nbatch=10000):
    _load_data(fn, session, target_table=CashBalance, converter=_cash_balance_converters, nbatch=nbatch)


def load_previous_application(fn, session, nbatch=10000):
    _load_data(fn, session, target_table=PreviousApplication, converter=_prev_application_converters, nbatch=nbatch)
