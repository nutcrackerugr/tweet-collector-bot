from telegram import Bot, ChatAction, Update
from telegram.ext import Updater, CallbackContext, CommandHandler

import collector
import config
import logging


streams = []

def start(update: Update, context: CallbackContext):
	if update.message.from_user.id in config.__LIST_OF_USERS__:
		context.bot.send_message(
			chat_id=update.effective_message.chat_id,
			text="Bienvenido de nuevo, {} ({}).\n" \
			"Comandos disponibles:\n  /query consulta [limite] - Volcado del" \
			"resultado de 'consulta' a un fichero.\n  /stream consulta " \
			"[limite] - Suscripción a stream de 'consulta'\n  /list - Lista" \
			" de streams activos\n  /stop n - Parar stream n".format(
				update.effective_user.first_name, 
				update.effective_user.id))


def query(update: Update, context: CallbackContext):
	if update.message.from_user.id in config.__LIST_OF_USERS__:
		query = update.message.text.split(" ", maxsplit=2)
		
		# Check if query string is correctly formed
		if len(query) >= 2:
			q = query[1]
			limit = int(query[2]) if len(query) == 3 else None
			
			# Status message
			context.bot.send_message(
				chat_id=update.effective_message.chat_id,
				text="Dumping from '{}'...".format(q))
			context.bot.send_chat_action(
				chat_id=update.effective_message.chat_id,
				action=ChatAction.UPLOAD_DOCUMENT)
			
			try:
				twitter = collector.StandardAPI()
				if limit:
					twitter.dump(q, limit=limit)
				else:
					twitter.dump(q)
			except Exception as e:
				context.bot.send_message(
					chat_id=update.effective_message.chat_id,
					text=str(e))
			
			context.bot.send_message(
				chat_id=update.effective_message.chat_id,
				text="Done dumping '{}'.".format(q))
		else:
			context.bot.send_message(
				chat_id=update.effective_message.chat_id,
				text="Malformed command")
			

def stream(update: Update, context: CallbackContext):
	if update.message.from_user.id in config.__LIST_OF_USERS__:
		query = update.message.text.split(" ", maxsplit=2)
		
		if len(query) >= 2:
			q = query[1]

			limit = int(query[2]) if len(query) == 3 else None

		
			try:
				stream = collector.StreamingAPI()

				if limit:
					stream.query(q, limit=limit)
				else:
					stream.query(q)
				
				streams.append((q, stream))
				context.bot.send_message(
					chat_id=update.effective_message.chat_id,
					text="Listening for '{}'...".format(q))
				
			except Exception as e:
				context.bot.send_message(
					chat_id=update.effective_message.chat_id,
					text=str(e))
			
		else:
			context.bot.send_message(
				chat_id=update.effective_message.chat_id,
				text="Malformed command")


def list_streams(update: Update, context: CallbackContext):
	if update.message.from_user.id in config.__LIST_OF_USERS__:
		response = "List of streams:\n"
		
		i = 0
		for stream in streams:
			i += 1
			response += "  {}. {} with {} tweets.\n".format(
				i, stream[0], len(stream[1].streamer.results))
		
		context.bot.send_message(
			chat_id=update.effective_message.chat_id, text=response)


def stop_stream(update: Update, context: CallbackContext):
	if update.message.from_user.id in config.__LIST_OF_USERS__:
		q = update.message.text.split(" ", maxsplit=1)
		
		if len(q) == 2:
			n = int(q[1]) - 1
			if n < len(streams) and n >= 0:
				streams[n][1].disconnect()
				streams[n][1].dump(streams[n][0])
			
				context.bot.send_message(
					chat_id=update.effective_message.chat_id,
					text="Done streaming '{}' with {} tweets.".format(
						streams[n][0], len(streams[n][1].streamer.results)))
				streams.pop(n)
			else:
				context.bot.send_message(
					chat_id=update.effective_message.chat_id,
					text="Invalid index")
		
		else:
			context.bot.send_message(
				chat_id=update.effective_message.chat_id,
				text="Malformed command")
	


if __name__ == "__main__":
	logging.basicConfig(
		format=config.__ERROR_FORMAT__, level=config.__ERROR_LEVEL__)
	
	collector.OAuthKeys.from_file("credentials.txt")
	
	bot = Bot(token=config.__TOKEN__)
	print(bot.get_me())
	
	updater = Updater(token=config.__TOKEN__, use_context=True)
	dispatcher = updater.dispatcher
	
	# Assign handlers to commands
	dispatcher.add_handler(CommandHandler("start", start))
	dispatcher.add_handler(CommandHandler("help", start))
	dispatcher.add_handler(CommandHandler("who", start))
	dispatcher.add_handler(CommandHandler("query", query))
	dispatcher.add_handler(CommandHandler("stream", stream))
	dispatcher.add_handler(CommandHandler("list", list_streams))
	dispatcher.add_handler(CommandHandler("stop", stop_stream))
	
	updater.start_polling()
	
