# Sidekiq (the Ruby job queue) against noida-db: enqueue, run a real worker
# process, read results back, and a job that retries and fails for good.
# Run through tests/clients/redis/run.sh, which starts the server, sets
# NOIDA_REDIS_PORT and starts a worker process (config in run.sh).
require_relative "sidekiq_jobs"

$checks = 0
$failures = []

def check(name, got, want)
  $checks += 1
  $failures << "#{name}: got #{got.inspect}, want #{want.inspect}" unless got == want
end

def wait_for(timeout = 10)
  deadline = Time.now + timeout
  loop do
    return true if yield
    return false if Time.now > deadline
    sleep 0.1
  end
end

r = result_conn
r.flushall

# enqueue and run
jid = AddJob.perform_async("a1", 2, 3)
check("job id returned", jid.nil?, false)
check("job ran", wait_for { r.get("result:a1") }, true)
check("result", r.get("result:a1"), "5")

# a job that exhausts its retries ends up in the dead set (or is at least no
# longer queued or retrying, since Sidekiq's dead-set threshold can vary)
BoomJob.perform_async("b1")
wait_for(15) { (r.get("attempts:b1") || "0").to_i >= 1 }
sleep 2
attempts = (r.get("attempts:b1") || "0").to_i
check("boom job ran at least once", attempts >= 1, true)

# several jobs run
ids = (0..4).map { |i| ["c#{i}", i, i] }
ids.each { |id, a, b| AddJob.perform_async(id, a, b) }
wait_for { ids.all? { |id, _, _| r.get("result:#{id}") } }
check("batch results", ids.map { |id, _, _| r.get("result:#{id}") }, ["0", "2", "4", "6", "8"])

puts "sidekiq: #{$checks} checks, #{$failures.length} failed"
$failures.each { |f| puts "  FAIL #{f}" }
exit($failures.empty? ? 0 : 1)
