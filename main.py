import datetime
import html
import time
import pandas
import requests

from collections import Counter
from logging import getLogger
from urllib.parse import urlparse

from django.db.models import Avg

from apps.carousel.models import Carousel
from apps.choices import CarouselStatus
from apps.utils import queryset_as_dataframe
from apps.sources.models import TildaLead, Lead
from config import settings

from apps.sources.management.commands._base import BaseCommand

logger = getLogger(__name__)
"""
данные ниже нужны тебе для того, чтобы это все работало
api token - получишь у bot father, когда бота создашь
chat_id - там чуть сложнее, его как-то можно вытянуть, но я не разобрался пока как, юзаю сторонний бот, чтобы вытянуть id нужный
bot_url - это из документации к телеге, открытой. Берем метод в зависимости от того ч хотим сделать вообще
"""
TELEGRAM_BOT_API_TOKEN = settings.TELEGRAM_BOT_API_TOKEN
TELEGRAM_BOT_GROUP_CHAT_ID = settings.TELEGRAM_BOT_GROUP_CHAT_ID
BOT_URL = f'https://api.telegram.org/bot{TELEGRAM_BOT_API_TOKEN}/sendMessage'


class Command(BaseCommand):
    help = "Отправка отчета в телеграм бота"

    def parse_url(self, value: str) -> bool:
        """
        предобработка данных
        :param value:
        :return:
        """
        parse_url = urlparse(html.unescape(value))
        url = parse_url.netloc + parse_url.path
        return True if 'baza' in url else False

    def first_report(self, df: datetime.datetime, dt: datetime.datetime) -> str:
        """
        Пример сборки части отчета
        :param df:
        :param dt:
        :return:
        """
        logger.info("  ↳Create first report")
        leads = queryset_as_dataframe(TildaLead.objects.filter(date_created__range=(df, dt)))
        leads['category'] = leads['roistat_url'].apply(self.parse_url)
        distribution = list(Carousel.objects.filter(created__range=(df, dt), distribution__range=(df, dt),
                                                    status__in=[CarouselStatus.complete.name,
                                                                CarouselStatus.qualified.name,
                                                                CarouselStatus.unqualified.name]).values_list(
            'owner__email', flat=True))
        qualified_count = Carousel.objects.filter(created__range=(df, dt), distribution__range=(df, dt),
                                                  status=CarouselStatus.qualified.name).count()
        unqualified_count = Carousel.objects.filter(created__range=(df, dt), distribution__range=(df, dt),
                                                    status=CarouselStatus.unqualified.name).count()
        avg_score_count = round(
            Carousel.objects.filter(created__range=(df, dt), distribution__range=(df, dt)).aggregate(Avg('score'))[
                'score__avg'])

        full_count = len(leads)
        baza_count = leads['category'].sum()
        paid_count = full_count - baza_count
        distribution_count = len(distribution)
        openers_count = (len(set(distribution)))
        lead_per_opener_count = round(distribution_count / openers_count)
        report_row = f'Отчет №1. Количество распределенных за вчера.\n\nКоличество пришедших лидов общее: {full_count}\nКоличество пришедших лидов база: {baza_count}\nКоличество пришедших лидов платный трафик: {paid_count}\nКоличество опенеров на смене: {openers_count}\nКоличество распределенных лидов: {distribution_count}\nКоличество распределенных на 1 опенера: {lead_per_opener_count}\nКоличество квал.лидов: {qualified_count}\nКоличество неквал.лидов: {unqualified_count}\nСредний балл распределенных лидов: {avg_score_count}\n'
        return report_row

    def second_report(self, df: datetime.datetime, dt: datetime.datetime) -> str:
        logger.info("  ↳Create second report")
        """
        тут какой то код формирования отчета
        """

        result_row = f'Отчет №2. Количество квал.лидов по каналам.\nОбщее\Квал\Неквал\n\nAI+GPT каналы: {paid_count}/{paid_qualified_count}/{paid_unqualified_count}\nBaza каналы: {baza_count}/{baza_qualified_count}/{baza_unqualified_count}\n'

        return result_row

    def third_report(self, df: datetime.datetime, dt: datetime.datetime) -> str:
        logger.info("  ↳Create third report")
       """
       тут какой то код формирования отчета
       """

        result_row = f'Отчет №3. Состав хвоста.\n\nВесь период:\nКоличество лидов с суммой баллов до 30: {score_lte30.count()}\nИз них количество дублей: {double_lte30}\nКоличество лидов с суммой баллов от 31: {score_gte31.count()}\nИз них количество дублей: {double_gte31}\n\nВчерашний день:\nКоличество лидов с суммой баллов до 30: {score_lte30_yesterday.count()}\nИз них количество дублей: {double_lte30_yesterday}\nКоличество лидов с суммой баллов от 31: {score_gte31_yesterday.count()}\nИз них количество дублей: {double_gte31_yesterday}\n'
        return result_row

    def send_telegram_message(self, text: str):
        """
        В ЭТОМ МЕСТЕ НЕПОСРЕДСТВЕННО ИДЕТ ОТПРАВКА ДАННЫХ В ТЕЛЕГРАММ БОТА, API МЕТОДОМ И ДАЛЕЕ ОН УЖЕ В ТЕЛЕГЕ ВЫВОДИТ СООБЩЕНИЕ
        В БОТЕ, ЧЕРЕЗ BOT FATHER НУЖНО НЕКОТОРЫЕ МАНИПУЛЯЦИИ СДЕЛАТЬ, Я ИХ ПОКАЖУ
        БОТА ДОБАВЛЯЕМ АДМИНОМ КАНАЛА
        в целом, если с ботом ты один будешь общаться, т.е. только ты должен получать фитбэк, то м.б. имеет смысл не париться с каналом,
        а усть он тебе и спамит напрямую в чате бота, без каналов всяких

        параметр chat_id - это в какой канал он должен кидать сообщение, важный момент
        text - что он должен передать (на сколько я помню, можно не только текст передать, но там чуть больше кода нужно)
        """
        data = {
            'chat_id': TELEGRAM_BOT_GROUP_CHAT_ID,
            'text': text
        }
        try:
            response = requests.post(BOT_URL, data=data)
            response.raise_for_status()
            logger.info("Telegram message sent successfully")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send Telegram message: {e}")

    def handle(self, **kwargs):
        logger.info("Telegram reporting start")
        today = datetime.datetime.now()
        df = (today - datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0,
                                                          tzinfo=datetime.timezone.utc)
        dt = today.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc)

        reports = [
            {'func': self.first_report, 'error_msg': 'Ошибка отчета №1'},
            {'func': self.second_report, 'error_msg': 'Ошибка отчета №2'},
            {'func': self.third_report, 'error_msg': 'Ошибка отчета №3'},
        ]

        for report_info in reports:
            try:
                report_result = report_info['func'](df, dt)
            except Exception as e:
                report_result = report_info['error_msg']
            self.send_telegram_message(report_result)
            time.sleep(4)

        logger.info("Sending reports")
