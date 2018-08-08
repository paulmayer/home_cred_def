from homeloans.tables import get_session, initialise_database
from homeloans.load_data import load_application_test, load_application_train, load_previous_application
from homeloans.load_data import load_bureau, load_bureau_balance
from homeloans.load_data import load_credit_card_balance
from homeloans.load_data import load_instalment_payments
from homeloans.load_data import load_cash_balance
from homeloans.config import DATA_DIR, DB_DIR
from homeloans.config import APPLICATION_TEST_FN, APPLICATION_TRAIN_FN, PREVIOUS_APPLICATION_FN
from homeloans.config import BUREAU_BALANCE_FN, BUREAU_FN
from homeloans.config import CREDIT_CARD_BALANCE_FN
from homeloans.config import INSTALMENTS_PAYMENTS_FN
from homeloans.config import POS_CASH_BALANCE_FN

import sys
import os

if __name__ == "__main__":
    initialise_database(os.path.join(DB_DIR, "home_loans.db"), drop=True)
    session = get_session()

    print('loading application training data.')
    try:
        load_application_train(fn=APPLICATION_TRAIN_FN, session=session)
        session.commit()
    except Exception as e:
        session.rollback()
        sys.stderr("Failed to load Application train data: %s" % e)
        sys.exit(-1)

    print('loading application test data.')
    try:
        load_application_test(fn=APPLICATION_TEST_FN, session=session)
        session.commit()
    except Exception as e:
        session.rollback()
        sys.stderr("Failed to load Application test data: %s" % e)
        sys.exit(-1)
    print('loading bureau data.')
    try:
        load_bureau(fn=BUREAU_FN, session=session)
        session.commit()
    except Exception as e:
        session.rollback()
        sys.stderr("Failed to load Bureau data: %s" % e)
        sys.exit(-1)

    print("loading bureau balance data.")
    try:
        load_bureau_balance(fn=BUREAU_BALANCE_FN, session=session)
        session.commit()
    except Exception as e:
        session.rollback()
        sys.stderr("Failed to load Bureau Balance data: %s" % e)
        sys.exit(-1)

    print("loading credit card balance data.")
    try:
        load_credit_card_balance(fn=CREDIT_CARD_BALANCE_FN, session=session)
        session.commit()
    except Exception as e:
        session.rollback()
        sys.stderr("Failed to load Credit Card Balance data: %s" % e)
        sys.exit(-1)
    print("loading instalment payments data.")
    try:
        load_instalment_payments(fn=INSTALMENTS_PAYMENTS_FN, session=session)
        session.commit()
    except Exception as e:
        session.rollback()
        sys.stderr("Failed to load Instalment Payments data: %s" % e)
        sys.exit(-1)

    print("loading cash balance data.")
    try:
        load_cash_balance(fn=POS_CASH_BALANCE_FN, session=session)
        session.commit()
    except Exception as e:
        session.rollback()
        sys.stderr("Failed to load Cash Balance data: %s" % e)
        sys.exit(-1)

    print("loading previous application data.")
    try:
        load_previous_application(fn=PREVIOUS_APPLICATION_FN, session=session)
        session.commit()
    except Exception as e:
        session.rollback()
        sys.stderr("Failed to load Previous Application test data: %s" % e)
        sys.exit(-1)