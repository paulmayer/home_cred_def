import os

MISSING_VALS = ("XNA", "")

DATA_DIR = "H:\\kaggle\\home_loans\\src"
DB_DIR = "H:\\kaggle\\home_loans\\db"
LOG_DIR = "H:\\kaggle\\home_loans\\log"

APPLICATION_TEST_FN = os.path.join(DATA_DIR, "application_test.csv")
APPLICATION_TRAIN_FN = os.path.join(DATA_DIR, "application_train.csv")
BUREAU_FN = os.path.join(DATA_DIR, "bureau.csv")
BUREAU_BALANCE_FN = os.path.join(DATA_DIR, "bureau_balance.csv")
CREDIT_CARD_BALANCE_FN = os.path.join(DATA_DIR, "credit_card_balance.csv")
INSTALMENTS_PAYMENTS_FN = os.path.join(DATA_DIR, "installments_payments.csv")
POS_CASH_BALANCE_FN = os.path.join(DATA_DIR, "POS_CASH_balance.csv")
PREVIOUS_APPLICATION_FN = os.path.join(DATA_DIR, "previous_application.csv")
