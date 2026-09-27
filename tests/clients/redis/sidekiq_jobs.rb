# Job classes for sidekiq_test.rb. Jobs write their result back into a Redis
# key (Sidekiq itself has no return-value channel) so the driver can poll it.
require "sidekiq"
require "redis"

Sidekiq.configure_client do |config|
  config.redis = { url: "redis://127.0.0.1:#{ENV.fetch('NOIDA_REDIS_PORT')}/0" }
end
Sidekiq.configure_server do |config|
  config.redis = { url: "redis://127.0.0.1:#{ENV.fetch('NOIDA_REDIS_PORT')}/0" }
end

def result_conn
  Redis.new(port: ENV.fetch("NOIDA_REDIS_PORT").to_i)
end

class AddJob
  include Sidekiq::Job

  def perform(id, a, b)
    result_conn.set("result:#{id}", (a + b).to_s)
  end
end

class BoomJob
  include Sidekiq::Job
  sidekiq_options retry: 2

  def perform(id)
    result_conn.incr("attempts:#{id}")
    raise "boom"
  end
end
