from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

from common import postgres_retrieve


@dag(
    schedule_interval=None,
    tags=["step-of-faith"],
)
def remind_of_seminars():
    @task
    def send_out(data: list):
        import telebot
        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

        bot = telebot.TeleBot(Variable.get("tg-token"))

        for (user_id, seminar) in data:
            if seminar is None:
                message = "До семинаров осталось меньше 15 минут. Самое время записаться!"
                keyboard = InlineKeyboardMarkup(row_width=1)
                keyboard.add(InlineKeyboardButton(text="В меню", callback_data="menu"))

            else:
                message = f"Семинар \"{seminar}\" начнётся в аудитории None уже менее чем через 15 минут!"
                keyboard = InlineKeyboardMarkup(row_width=2)
                keyboard.add(InlineKeyboardButton(text="Семинары", callback_data="seminar_registration"))
                keyboard.add(InlineKeyboardButton(text="В меню", callback_data="menu"))

            bot.send_message(user_id, message, reply_markup=keyboard)
            
   
    q = "select user_id, seminar from step_of_faith.users"
    send_out(postgres_retrieve(q))


remind_of_seminars()

