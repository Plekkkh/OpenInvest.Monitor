from t_tech.invest import OperationType

OPERATION_MAPPING = {
    # Сделки
    OperationType.OPERATION_TYPE_BUY: 'buy',
    OperationType.OPERATION_TYPE_BUY_CARD: 'buy',
    OperationType.OPERATION_TYPE_BUY_MARGIN: 'buy',
    OperationType.OPERATION_TYPE_DELIVERY_BUY: 'buy',
    OperationType.OPERATION_TYPE_SELL: 'sell',
    OperationType.OPERATION_TYPE_SELL_CARD: 'sell',
    OperationType.OPERATION_TYPE_SELL_MARGIN: 'sell',
    OperationType.OPERATION_TYPE_DELIVERY_SELL: 'sell',
    OperationType.OPERATION_TYPE_BOND_REPAYMENT_FULL: 'repayment',

    # Начисления
    OperationType.OPERATION_TYPE_DIVIDEND: 'dividend',
    OperationType.OPERATION_TYPE_DIV_EXT: 'dividend',
    OperationType.OPERATION_TYPE_DIVIDEND_TRANSFER: 'dividend',
    OperationType.OPERATION_TYPE_COUPON: 'coupon',
    OperationType.OPERATION_TYPE_BOND_REPAYMENT: 'amortization',

    # Расходы
    OperationType.OPERATION_TYPE_BROKER_FEE: 'commission',
    OperationType.OPERATION_TYPE_SERVICE_FEE: 'commission',
    OperationType.OPERATION_TYPE_MARGIN_FEE: 'commission',
    OperationType.OPERATION_TYPE_SUCCESS_FEE: 'commission',
    OperationType.OPERATION_TYPE_TRACK_MFEE: 'commission',
    OperationType.OPERATION_TYPE_TRACK_PFEE: 'commission',
    OperationType.OPERATION_TYPE_CASH_FEE: 'commission',
    OperationType.OPERATION_TYPE_OUT_FEE: 'commission',
    OperationType.OPERATION_TYPE_OUT_STAMP_DUTY: 'commission',
    OperationType.OPERATION_TYPE_OUTPUT_PENALTY: 'commission',
    OperationType.OPERATION_TYPE_ADVICE_FEE: 'commission',
    OperationType.OPERATION_TYPE_OTHER_FEE: 'commission',
    OperationType.OPERATION_TYPE_OVER_COM: 'commission',

    OperationType.OPERATION_TYPE_TAX: 'tax',
    OperationType.OPERATION_TYPE_BOND_TAX: 'tax',
    OperationType.OPERATION_TYPE_DIVIDEND_TAX: 'tax',
    OperationType.OPERATION_TYPE_BENEFIT_TAX: 'tax',
    OperationType.OPERATION_TYPE_TAX_CORRECTION: 'tax',
    OperationType.OPERATION_TYPE_TAX_PROGRESSIVE: 'tax',
    OperationType.OPERATION_TYPE_BOND_TAX_PROGRESSIVE: 'tax',
    OperationType.OPERATION_TYPE_DIVIDEND_TAX_PROGRESSIVE: 'tax',
    OperationType.OPERATION_TYPE_BENEFIT_TAX_PROGRESSIVE: 'tax',
    OperationType.OPERATION_TYPE_TAX_CORRECTION_PROGRESSIVE: 'tax',
    OperationType.OPERATION_TYPE_TAX_REPO_PROGRESSIVE: 'tax',
    OperationType.OPERATION_TYPE_TAX_REPO: 'tax',
    OperationType.OPERATION_TYPE_TAX_REPO_HOLD: 'tax',
    OperationType.OPERATION_TYPE_TAX_REPO_HOLD_PROGRESSIVE: 'tax',
    OperationType.OPERATION_TYPE_TAX_CORRECTION_COUPON: 'tax',

    OperationType.OPERATION_TYPE_TAX_REPO_REFUND: 'tax_refund',
    OperationType.OPERATION_TYPE_TAX_REPO_REFUND_PROGRESSIVE: 'tax_refund',

    # Валюта
    OperationType.OPERATION_TYPE_INPUT: 'deposit',
    OperationType.OPERATION_TYPE_INP_MULTI: 'deposit',
    OperationType.OPERATION_TYPE_INPUT_SWIFT: 'deposit',
    OperationType.OPERATION_TYPE_INPUT_ACQUIRING: 'deposit',

    OperationType.OPERATION_TYPE_OUTPUT: 'withdrawal',
    OperationType.OPERATION_TYPE_OUT_MULTI: 'withdrawal',
    OperationType.OPERATION_TYPE_OUTPUT_SWIFT: 'withdrawal',
    OperationType.OPERATION_TYPE_OUTPUT_ACQUIRING: 'withdrawal',

    OperationType.OPERATION_TYPE_OVERNIGHT: 'other_income',
    OperationType.OPERATION_TYPE_OVER_INCOME: 'other_income',
    OperationType.OPERATION_TYPE_ACCRUING_VARMARGIN: 'other_income',
    OperationType.OPERATION_TYPE_WRITING_OFF_VARMARGIN: 'other_expense',
}

INSTRUMENT_TYPE_MAPPING = {
    'share': 'Share',
    'bond': 'Bond',
    'etf': 'ETF',
    'currency': 'Currency',
}
