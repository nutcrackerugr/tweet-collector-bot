import telegram
from telegram.ext import Updater, CommandHandler

import collector
import config
import logging


streams = []

def start(bot, update):
	if update.message.from_user.id in config.__LIST_OF_USERS__:
		bot.send_message(chat_id=update.effective_message.chat_id, text="Bienvenido de nuevo, {} ({}).\nComandos disponibles:\n  /query consulta - Volcado del resultado de 'consulta' a un fichero.\n  /stream consulta - Suscripción a stream de 'consulta'\n  /list - Lista de streams activos\n  /stop n - Parar stream n".format(update.effective_user.first_name, update.effective_user.id))

def query(bot, update):
	if update.message.from_user.id in config.__LIST_OF_USERS__:
		q = update.message.text.split(" ", maxsplit=1)
		
		if len(q) == 2:
			q = q[1]
			bot.send_message(chat_id=update.effective_message.chat_id, text="Dumping '{}'...".format(q))
			bot.send_chat_action(chat_id=update.effective_message.chat_id, action=telegram.ChatAction.UPLOAD_DOCUMENT)
			
			try:
				twitter = collector.StandardAPI()
				twitter.dump(q)
			except Exception as e:
				bot.send_message(chat_id=update.effective_message.chat_id, text=str(e))
			
			bot.send_message(chat_id=update.effective_message.chat_id, text="Done dumping '{}'.".format(q))
		else:
			bot.send_message(chat_id=update.effective_message.chat_id, text="Malformed command")
			

def stream(bot, update):
	if update.message.from_user.id in config.__LIST_OF_USERS__:
		q = update.message.text.split(" ", maxsplit=1)
		
		if len(q) == 2:
			q = q[1]
		
			try:
				stream = collector.StreamingAPI()
				stream.query(q)
				streams.append((q, stream))
				bot.send_message(chat_id=update.effective_message.chat_id, text="Listening for '{}'...".format(q))
				
			except Exception as e:
				bot.send_message(chat_id=update.effective_message.chat_id, text=str(e))
			
		else:
			bot.send_message(chat_id=update.effective_message.chat_id, text="Malformed command")


def list_streams(bot, update):
	if update.message.from_user.id in config.__LIST_OF_USERS__:
		response = "List of streams:\n"
		
		i = 0
		for stream in streams:
			i += 1
			response += "  {}. {} with {} tweets.\n".format(i, stream[0], len(stream[1].streamer.results))
		
		bot.send_message(chat_id=update.effective_message.chat_id, text=response)


def stop_stream(bot, update):
	if update.message.from_user.id in config.__LIST_OF_USERS__:
		q = update.message.text.split(" ", maxsplit=1)
		
		if len(q) == 2:
			n = int(q[1]) - 1
			if n < len(streams) and n >= 0:
				streams[n][1].disconnect()
				streams[n][1].dump(streams[n][0])
			
				bot.send_message(chat_id=update.effective_message.chat_id, text="Done streaming '{}' with {} tweets.".format(streams[n][0], len(streams[n][1].streamer.results)))
				streams.pop(n)
			else:
				bot.send_message(chat_id=update.effective_message.chat_id, text="Invalid index")
		
		else:
			bot.send_message(chat_id=update.effective_message.chat_id, text="Malformed command")
	


if __name__ == "__main__":
	logging.basicConfig(format=config.__ERROR_FORMAT__, level=config.__ERROR_LEVEL__)
	
	collector.OAuthKeys.from_file("credentials.txt")
	
	bot = telegram.Bot(token=config.__TOKEN__)
	print(bot.get_me())
	
	updater = Updater(token=config.__TOKEN__)
	dispatcher = updater.dispatcher
	
	dispatcher.add_handler(CommandHandler("start", start))
	dispatcher.add_handler(CommandHandler("help", start))
	dispatcher.add_handler(CommandHandler("who", start))
	dispatcher.add_handler(CommandHandler("query", query))
	dispatcher.add_handler(CommandHandler("stream", stream))
	dispatcher.add_handler(CommandHandler("list", list_streams))
	dispatcher.add_handler(CommandHandler("stop", stop_stream))
	
	updater.start_polling()
	
