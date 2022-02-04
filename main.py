import asyncio
import bitmex
import logging
import traceback
import json
import pymysql
from pymysql.cursors import DictCursor
from aiogram import Bot, Dispatcher, executor, types
from bravado.exception import HTTPError as bravex
from config import *

CLIENT = bitmex.bitmex(test=True,
                       api_key=CL_ID,
                       api_secret=CL_SECRET)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=TG_BOT_TOKEN, )
dp = Dispatcher(bot=bot, )

USER_ACTIVE_BOTS = {}
USER_ACTIVE_ORDERS = {}
IS_END = {}

settings = {}

set_num = {}


def main():
    executor.start_polling(dispatcher=dp, )


def current_options(chat_id):
    return f'Buy_price: {settings[chat_id]["buy_price"]}\n' \
           f'Sell_price: {settings[chat_id]["sell_price"]}\n' \
           f'Quantity: {settings[chat_id]["qty"]}\n' \
           f'Currency: {settings[chat_id]["cur"]}'


def keyboard_settings():
    markup = types.ReplyKeyboardMarkup(one_time_keyboard=False, resize_keyboard=True)
    btn1 = types.KeyboardButton('Buy Price')
    btn2 = types.KeyboardButton('Sell Price')
    btn3 = types.KeyboardButton('Quantity')
    markup.add(btn1, btn2, btn3)
    return markup


def keyboard_cur_bots(chat_id):
    markup = types.InlineKeyboardMarkup(row_width=3)
    counter = 0
    for i in USER_ACTIVE_BOTS[chat_id]:
        if USER_ACTIVE_BOTS[chat_id][i]:
            counter += 1
            btn = types.InlineKeyboardButton(text=i, callback_data='cur' + i)
            markup.insert(btn)
        else:
            continue
    if counter == 0:
        return 0
    else:
        return markup


async def keyboard_all_bots():
    async def get_all_future_instruments():
        while True:
            try:
                future_instruments = CLIENT.Instrument.Instrument_getActiveAndIndices().result()[0]
                currencies = []
                for i in future_instruments:
                    if i['state'] == 'Open':
                        currencies.append(i['symbol'])
                return currencies
            except Exception:
                await asyncio.sleep(2)
                print('Something get wrong!')
                print(traceback.format_exc())
                continue

    answer = await get_all_future_instruments()
    markup = types.InlineKeyboardMarkup(row_width=2)
    for cur_name in answer:
        btn = types.InlineKeyboardButton(text=cur_name, callback_data=cur_name)
        markup.insert(btn)
    return markup


async def buy(cur, qty, price):
    order = CLIENT.Order.Order_new(symbol=f'{cur}',
                                   side='Buy',
                                   orderQty=f'{qty}',
                                   price=f'{price}',
                                   ordType='Limit',
                                   timeInForce='GoodTillCancel').result()
    answer = order[0]['orderID']
    return answer


async def sell(cur, qty, price):
    order = CLIENT.Order.Order_new(symbol=f'{cur}',
                                   side='Sell',
                                   orderQty=f'{qty}',
                                   price=f'{price}',
                                   ordType='Limit',
                                   timeInForce='GoodTillCancel').result()
    answer = order[0]['orderID']
    return answer


async def cancel_by_currency(chat_id, cur):
    for i in USER_ACTIVE_ORDERS[chat_id][cur]:
        try:
            e = CLIENT.Order.Order_cancel(orderID=i).result()
        except bravex:
            continue
        if not e:
            return False
    return True


async def get_user_trades_by_order(order_id_fun_buy, order_id_fun_sell, chat_id, cur):
    if order_id_fun_buy == 0:
        order_id_fun_buy = '0'
    if order_id_fun_sell == 0:
        order_id_fun_sell = '0'
    while True:
        if not IS_END[chat_id][cur]:
            try:
                buy_order = CLIENT.Order.Order_getOrders(filter=json.dumps({"orderID": order_id_fun_buy})).result()
                await asyncio.sleep(1)
                sell_order = CLIENT.Order.Order_getOrders(filter=json.dumps({"orderID": order_id_fun_sell})).result()
                if order_id_fun_buy != '0':
                    if buy_order[0][0]['ordStatus'] == 'Filled':
                        # await bot.send_message(448522410,
                        #                        f'виконався buy по {cur}')
                        await cancel_by_currency(chat_id, cur)
                        # await bot.send_message(448522410,
                        #                        f'затер sell по {cur} ')
                        return 1, buy_order[0]
                    else:
                        print('Buy order status — ', buy_order[0][0]['ordStatus'],
                              f'orderID: {order_id_fun_buy}, cur: {cur}')
                if order_id_fun_sell != '0':
                    if sell_order[0][0]['ordStatus'] == 'Filled':
                        # await bot.send_message(448522410,
                        #                        f'виконався sell по {cur}')
                        await cancel_by_currency(chat_id, cur)
                        # await bot.send_message(448522410,
                        #                        f'затер buy по {cur} ')
                        return 2, sell_order[0]
                    else:
                        print('Sell order status — ', sell_order[0][0]['ordStatus'],
                              f'orderID: {order_id_fun_sell}, cur: {cur}')
                await asyncio.sleep(1)
            except Exception:
                continue
        else:
            await bot.send_message(448522410,
                                   f'{cur} — stopped ')
            return 4, 'nothing'


@dp.message_handler(commands=['start'])
async def start_message(message: types.Message):
    await bot.send_message(message.chat.id, f'Welcome, your Chat_id is {message.chat.id}')
    settings[message.chat.id] = {
        'buy_price': 0,
        'sell_price': 0,
        'qty': 0,
        'cur': 0
    }
    USER_ACTIVE_ORDERS[message.chat.id] = []
    USER_ACTIVE_BOTS[message.chat.id] = []
    IS_END[message.chat.id] = []
    await bot.send_message(message.chat.id, 'Current options:')
    await bot.send_message(message.chat.id, current_options(message.chat.id),
                           reply_markup=keyboard_settings())


@dp.message_handler(commands=['select'])
async def select_bot(message: types.Message):
    await bot.send_message(message.chat.id, 'Please, choose the instrument', reply_markup=await keyboard_all_bots())


@dp.message_handler(commands=['stop'])
async def stop_message(message: types.Message):
    current_bots = keyboard_cur_bots(message.chat.id)
    if current_bots == 0:
        await bot.send_message(message.chat.id,
                               'There are any running bots -----> /select')
    else:
        await bot.send_message(message.chat.id,
                               'Select what you want to turn off',
                               reply_markup=keyboard_cur_bots(message.chat.id))


@dp.message_handler(commands=['go'])
async def go_message(message: types.Message):
    settings_cur = settings[message.chat.id].copy()
    connection = pymysql.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        db=DB,
        charset='utf8mb4',
        cursorclass=DictCursor,
        autocommit=True
    )
    try:
        info = connection.cursor()
        info.execute(f"select * from bitmex_oleh where cur = '{settings_cur['cur']}' ")
        rows = info.fetchall()
        if rows:
            query = "Update bitmex_oleh Set " \
                    f"buy_price = {str(settings_cur['buy_price'])}," \
                    f"sell_price = {str(settings_cur['sell_price'])}," \
                    f"quant = {str(settings_cur['qty'])} " \
                    f"WHERE cur = '{str(settings_cur['cur'])}'"
            connection.cursor().execute(query)
            connection.commit()
        else:
            query = "INSERT INTO bitmex_oleh(cur, buy_price, sell_price, quant) " \
                    f"VALUES ('{settings_cur['cur']}'," \
                    f"{str(settings_cur['buy_price'])}," \
                    f"{str(settings_cur['sell_price'])}," \
                    f"{str(settings_cur['qty'])})"
        connection.cursor().execute(query)
        connection.commit()
    except Exception:
        pass
        # await bot.send_message(message.chat.id, 'Can\'t connect to database')
    connection.close()
    USER_ACTIVE_ORDERS[message.chat.id] = {settings_cur['cur']: ['1', '2']}
    USER_ACTIVE_BOTS[message.chat.id] = {settings_cur['cur']: True}
    IS_END[message.chat.id] = {settings_cur['cur']: False}

    await bot.send_message(message.chat.id, 'I am running, check the exchange')
    step = (settings_cur["sell_price"] - settings_cur["buy_price"]) / 2
    while True:
        try:
            buy_number = await buy(cur=settings_cur['cur'],
                                   qty=settings_cur['qty'],
                                   price=settings_cur['buy_price'])
            USER_ACTIVE_ORDERS[message.chat.id][settings_cur['cur']][0] = buy_number
            print('USER_ACTIVE_ORDERS:', USER_ACTIVE_ORDERS)
        except Exception:
            print(traceback.format_exc())
            return

        try:
            sell_number = await sell(cur=settings_cur['cur'],
                                     qty=settings_cur['qty'],
                                     price=settings_cur['sell_price'])
            USER_ACTIVE_ORDERS[message.chat.id][settings_cur['cur']][1] = sell_number
        except Exception:
            print(traceback.format_exc())
            return

        if buy_number or sell_number:
            try:
                response, result = await get_user_trades_by_order(order_id_fun_buy=buy_number,
                                                                  order_id_fun_sell=sell_number,
                                                                  chat_id=message.chat.id,
                                                                  cur=settings_cur['cur'])
            except Exception:

                print(traceback.format_exc())

                return
            if response == 1:
                settings_cur['sell_price'] -= step
                settings_cur['buy_price'] -= step

            elif response == 2:
                settings_cur['sell_price'] += step
                settings_cur['buy_price'] += step

            elif response == 4:
                await bot.send_message(message.chat.id,
                                       f'I am stopped on {settings_cur["cur"]}',
                                       reply_markup=keyboard_settings())
                return

            else:
                await bot.send_message(message.chat.id, 'Глюкануло, дьорни Олега')
                await bot.send_message(448522410, 'новий респонз')
                return


@dp.message_handler(content_types=['text'])
async def text_message(message: types.Message):
    try:
        if message.text == 'Buy Price':
            await bot.send_message(message.chat.id, 'Insert Buy price')
            set_num[message.chat.id] = 1
            return
        elif message.text == 'Sell Price':
            await bot.send_message(message.chat.id, 'Insert Sell price')
            set_num[message.chat.id] = 2
            return
        elif message.text == 'Quantity':
            await bot.send_message(message.chat.id, 'Insert Quantity')
            set_num[message.chat.id] = 3
            return
        else:

            try:
                text = message.text
                text = text.replace(',', '.')
                number = float(text)
            except Exception:
                await bot.send_message(message.chat.id, 'Invalid data')
                await bot.send_message(message.chat.id, 'Current options:')
                await bot.send_message(message.chat.id,
                                       current_options(message.chat.id),
                                       reply_markup=keyboard_settings())
                set_num[message.chat.id] = 0
                return

            if set_num[message.chat.id] == 1:
                settings[message.chat.id]['buy_price'] = number
            elif set_num[message.chat.id] == 2:
                settings[message.chat.id]['sell_price'] = number
            elif set_num[message.chat.id] == 3:
                settings[message.chat.id]['qty'] = int(number)
            else:
                raise ValueError

        await bot.send_message(message.chat.id, 'Current options:')
        await bot.send_message(message.chat.id,
                               current_options(message.chat.id),
                               reply_markup=keyboard_settings())
        set_num[message.chat.id] = 0

    except Exception:
        await bot.send_message(message.chat.id,
                               'Something get wrong')
        await bot.send_message(message.chat.id, 'Current options:')
        await bot.send_message(message.chat.id, current_options(message.chat.id), reply_markup=keyboard_settings())
        set_num[message.chat.id] = 0


@dp.callback_query_handler()
async def callback_inline(call):
    if 'cur' in call.data:
        currency = call.data[3:]
        await bot.send_message(call.message.chat.id, currency + '— stopping')
        IS_END[call.message.chat.id][currency] = True
        USER_ACTIVE_BOTS[call.message.chat.id][currency] = False
        try:
            await cancel_by_currency(call.message.chat.id, currency)
        except Exception:
            return
        await bot.send_message(call.message.chat.id, "press on ----> /select, and choose new pair")

    else:
        market_price = CLIENT.Instrument.Instrument_getActiveAndIndices().result()
        connection = pymysql.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            db=DB,
            charset='utf8mb4',
            cursorclass=DictCursor,
            autocommit=True
        )
        info = connection.cursor()
        info.execute(f"select * from bitmex_oleh where cur = '{call.data}' ")
        rows = info.fetchall()
        if rows:
            for row in rows:
                settings[call.message.chat.id]['cur'] = row['cur']
                settings[call.message.chat.id]['buy_price'] = row['buy_price']
                settings[call.message.chat.id]['sell_price'] = row['sell_price']
                settings[call.message.chat.id]['qty'] = row['quant']
                await bot.send_message(call.message.chat.id,
                                       current_options(call.message.chat.id),
                                       reply_markup=keyboard_settings())

        else:
            await bot.send_message(call.message.chat.id, 'Please, enter values')
            settings[call.message.chat.id]['cur'] = call.data
            settings[call.message.chat.id]['buy_price'] = 0.0
            settings[call.message.chat.id]['sell_price'] = 0.0
            settings[call.message.chat.id]['qty'] = 0
            await bot.send_message(call.message.chat.id,
                                   current_options(call.message.chat.id),
                                   reply_markup=keyboard_settings())
        connection.close()
        for i in market_price[0]:
            if i['symbol'] == f'{call.data}':
                await bot.send_message(call.message.chat.id, f'{i["lastPrice"]} — last price of {i["symbol"]}')
        return


if __name__ == '__main__':
    main()
