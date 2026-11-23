Rails.application.config.active_job.queue_adapter = :solid_queue
Rails.application.config.solid_queue.silence_polling = true
Rails.application.config.log_level = :warn
