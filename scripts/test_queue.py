from app.queue.redis_queue import RedisQueue

queue = RedisQueue()

queue.enqueue("job101")
queue.enqueue("job102")

print(queue.size())

print(queue.dequeue())
print(queue.dequeue())

print(queue.size())