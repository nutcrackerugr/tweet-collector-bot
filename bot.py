from telegram import Bot, ChatAction, Update
from telegram.ext import Updater, CallbackContext, CommandHandler

import collector
import config
import logging

import json
import datetime
import time

import tweepy

from multiprocessing import Process

MAX_DEPTH = 3
streams = []
queue = None
task_consumer = None

def task_consumer_worker(queue):
	finished = False
	results = list()
	errors = list()

	credentials = collector.OAuthKeys.get()
	if credentials:
		ck, cs, k, s = credentials
		auth = tweepy.OAuthHandler(ck, cs)
		auth.set_access_token(k, s)
	else:
		raise Exception("No free credentials")

	api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

	while not finished:
		task = queue.get_task()

		print(f"TASK: {task.task}")

		if task.task == "FINISH":
			finished = True
		elif task.depth <= MAX_DEPTH:
			if task.task[:3] == "tid":
				try:
					status = api.get_status(task.task[4:])
					if status:
						results.append(status._json)

						if hasattr(status, "retweeted_status"):
							queue.add_task(collector.Task(f"tid:{status.retweeted_status.id_str}", task.depth))
							queue.add_task(collector.Task(f"uid:{status.retweeted_status.user.id_str}", task.depth))
						
						if status.in_reply_to_status_id_str:
							queue.add_task(collector.Task(f"tid:{status.in_reply_to_status_id_str}", task.depth))
							queue.add_task(collector.Task(f"uid:{status.in_reply_to_user_id_str}", task.depth))
				except Exception as e:
					print(f"{task.task}: {str(e)}")
					errors.append(task.task)

			elif task.task[:3] == "uid":
				try:
					statuses = api.user_timeline(id=task.task[4:], count=100)

					for status in statuses:
						results.append(status._json)

						if hasattr(status, "retweeted_status"):
							queue.add_task(collector.Task(f"tid:{status.retweeted_status.id_str}", task.depth))
							queue.add_task(collector.Task(f"uid:{status.retweeted_status.user.id_str}", task.depth))
						
						if status.in_reply_to_status_id_str:
							queue.add_task(collector.Task(f"tid:{status.in_reply_to_status_id_str}", task.depth))
							queue.add_task(collector.Task(f"uid:{status.in_reply_to_user_id_str}", task.depth))
				except Exception as e:
					print(f"{task.task}: {str(e)}")
					errors.append(task.task)

	
	filepath = "dumps/@CtrlSec_tasks_{}.{}.json".format(
		datetime.datetime.now().strftime("%Y%m%d-%H%M%S"),
		"stream")
	with open(filepath, "w+") as f:
		json.dump(results, f, indent=2)

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


def monitor_ctrlsec(update: Update, context: CallbackContext):
	global queue, task_consumer

	if update.message.from_user.id in config.__LIST_OF_USERS__:
		query = update.message.text.split(" ", maxsplit=2)
		queue = collector.TaskQueue()
		task_consumer = Process(target=task_consumer_worker, args=(queue,))
		task_consumer.start()

		try:
			stream = collector.StreamingAPI(streamer=collector.ListenerStreamHandler(queue))
			
			q = "185731569" #manu3lf
			stream.query(q, is_user=True)
			
			streams.append(("@CtrlSec", stream))
			context.bot.send_message(
				chat_id=update.effective_message.chat_id,
				text="Listening for '@CtrlSec'...")
			
		except Exception as e:
			context.bot.send_message(
				chat_id=update.effective_message.chat_id,
				text=str(e))

def stop_monitoring(update: Update, context: CallbackContext):
	global queue, task_consumer

	if update.message.from_user.id in config.__LIST_OF_USERS__:
		for key, stream in enumerate(streams):
			if stream[0] == "@CtrlSec":
				stream[1].disconnect()
				streams.pop(key)
				stream[1].dump("@CtrlSec_monitoring")
				queue.add_task(collector.Task("FINISH", 0))

				context.bot.send_message(
					chat_id=update.effective_message.chat_id,
					text="Attempting gracefully shutdown")

				task_consumer.join(60)

				context.bot.send_message(
					chat_id=update.effective_message.chat_id,
					text="Done monitoring")
				break

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
	dispatcher.add_handler(CommandHandler("monitor", monitor_ctrlsec))
	dispatcher.add_handler(CommandHandler("stop_monitoring", stop_monitoring))
	
	updater.start_polling()
	
