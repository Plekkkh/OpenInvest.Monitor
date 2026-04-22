from django.db import models
from django.contrib.auth.models import User
from cryptography.fernet import Fernet
from django.conf import settings

# Инициализация Fernet с ключом из настроек
fernet = Fernet(settings.FERNET_KEY)


class Asset(models.Model):
    """Справочник активов (акции, облигации и т.д.)"""
    TYPE_CHOICES = [
        ('Share', 'Акция'),
        ('Bond', 'Облигация'),
        ('ETF', 'Фонд'),
        ('Currency', 'Валюта'),
    ]

    instrument_uid = models.CharField(
        max_length=64,
        unique=True,
        blank=True,
        null=True,
        verbose_name="UID инструмента у брокера"
    )
    figi = models.CharField(
        max_length=32,
        unique=True,
        blank=True,
        null=True,
        verbose_name="FIGI инструмента"
    )
    ticker = models.CharField(max_length=20, verbose_name="Тикер")
    isin = models.CharField(max_length=12, blank=True, null=True, verbose_name="ISIN")
    name = models.CharField(max_length=255, verbose_name="Название")
    asset_type = models.CharField(max_length=20, choices=TYPE_CHOICES, verbose_name="Тип актива")
    currency = models.CharField(max_length=10, default='RUB', verbose_name="Валюта")

    def __str__(self):
        return f"{self.ticker} ({self.name})"

    class Meta:
        verbose_name = "Актив"
        verbose_name_plural = "Активы"


class BrokerAccount(models.Model):
    """Брокерский счет пользователя с зашифрованным API-токеном"""
    PROVIDER_CHOICES = [
        ('T-Invest_API', 'Т-Инвестиции (API)'),
        ('Manual', 'Ручной ввод'),
    ]

    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='accounts', verbose_name="Пользователь")
    name = models.CharField(max_length=100, verbose_name="Название счета")
    provider_type = models.CharField(max_length=20, choices=PROVIDER_CHOICES, verbose_name="Провайдер")
    provider_account_id = models.CharField(
        max_length=100, unique=True, blank=True, null=True, verbose_name="ID счета у провайдера"
    )
    _encrypted_token = models.BinaryField(blank=True, null=True, verbose_name="Зашифрованный API-токен")

    @property
    def api_token(self):
        """Расшифровывает и возвращает API-токен, если он есть"""
        if self._encrypted_token:
            return fernet.decrypt(bytes(self._encrypted_token)).decode()
        return None

    @api_token.setter
    def api_token(self, raw_token):
        """При установке API-токена, он шифруется и сохраняется в базе"""
        if raw_token:
            self._encrypted_token = fernet.encrypt(raw_token.encode())
        else:
            self._encrypted_token = None

    @property
    def masked_token(self):
        """Возвращает замаскированный токен вида t.v2...abcd"""
        token = self.api_token
        if token and len(token) > 8:
            return f"{token[:4]}...{token[-4:]}"
        return None

    def __str__(self):
        return f"{self.name} ({self.user.username})"

    class Meta:
        verbose_name = "Брокерский счет"
        verbose_name_plural = "Брокерские счета"


class Transaction(models.Model):
    """
    Атомарная операция — покупка, продажа, дивиденды, налоги и т.д.

    База для всех расчетов. Каждая транзакция связана с конкретным активом и брокерским счетом.
    """
    OPERATION_CHOICES = [
        # Сделки (затрагивают активы)
        ('buy', 'Покупка'),
        ('sell', 'Продажа'),
        ('repayment', 'Погашение облигаций'),
        # Начисления (полученная выгода)
        ('dividend', 'Дивиденды'),
        ('coupon', 'Купоны'),
        ('amortization', 'Амортизационные выплаты'),
        ('other_accrual', 'Прочие начисления'),
        # Расходы (уменьшение баланса)
        ('commission', 'Комиссия'),
        ('tax', 'Налог'),
        ('tax_refund', 'Возврат налога'),
        ('expense', 'Расход'),
        # Валюта (движение средств)
        ('deposit', 'Пополнение счета'),
        ('withdrawal', 'Вывод средств'),
        ('other_income', 'Прочие доходы'),
        ('other_expense', 'Прочие расходы'),
        ('conversion', 'Конвертация'),
        ('conversion_commission', 'Комиссия на конвертацию'),
    ]

    account = models.ForeignKey(BrokerAccount, on_delete=models.CASCADE, related_name='transactions')
    external_id = models.CharField(
        max_length=255, unique=True, blank=True, null=True, verbose_name="Внешний ID (от брокера)"
    )
    asset = models.ForeignKey(Asset, on_delete=models.CASCADE, null=True, blank=True)
    operation_type = models.CharField(max_length=30, choices=OPERATION_CHOICES, verbose_name="Тип операции")
    quantity = models.DecimalField(max_digits=15, decimal_places=6, default=0, verbose_name="Количество")
    price_per_unit = models.DecimalField(max_digits=15, decimal_places=4, default=0, verbose_name="Цена за единицу")
    date = models.DateTimeField(verbose_name="Дата операции")

    yield_amount = models.DecimalField(
        max_digits=15, decimal_places=4, default=0, verbose_name="Финансовый результат (доходность)"
    )
    commission_amount = models.DecimalField(
        max_digits=15, decimal_places=4, default=0, verbose_name="Удержанная комиссия"
    )
    accrued_int = models.DecimalField(
        max_digits=15, decimal_places=4, default=0, verbose_name="НКД (накопленный купонный доход)"
    )

    parent_transaction = models.ForeignKey(
        'self',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='child_transactions',
        verbose_name="Родительская операция"
    )

    @property
    def total_amount(self):
        """Общая сумма операции (quantity * price_per_unit)"""
        return self.quantity * self.price_per_unit

    def __str__(self):
        asset_info = self.asset.ticker if self.asset else "Счет"
        return f"{self.get_operation_type_display()} {asset_info} — {self.total_amount} RUB"

    class Meta:
        verbose_name = "Транзакция"
        verbose_name_plural = "Транзакции"
        ordering = ['-date']
