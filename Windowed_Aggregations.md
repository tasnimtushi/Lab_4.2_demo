# Lab 4.2 — Windowed Aggregations


## Introduction

Streaming systems receive events continuously. Instead of counting all events together, windowed aggregation groups events into time windows and calculates a result for each window.

In this lab, you will use **Apache Flink with PyFlink** to group events by `user_id` and calculate event counts using:

- a **5-minute tumbling window**
- a **10-minute sliding window**

The event data is generated inside the lab, so no Kafka cluster is required.

**Documentation note:** Add the indicated screenshot after each practical step so students can match the instruction with the expected screen.

![Windowed Aggregations Architecture](lab_4_2_windowed_aggregations_diagram.svg)

## Learning Objectives

By the end of this lab, you will be able to:

1. Create a simple Flink streaming job.
2. Group events by `user_id`.
3. Apply a 5-minute tumbling window.
4. Apply a 10-minute sliding window.
5. Count events for each user in each window.
6. Compare tumbling and sliding window results.

**Prerequisites:** Basic Python knowledge and basic understanding of streaming events.

## Prologue: The Challenge

You are working on a DataOps team that receives user activity events.

The team wants to answer questions such as:

> How many events did each user generate during a 5-minute period?

It also needs a moving view:

> How many events did each user generate during the latest 10-minute period?

Your task is to build a small Flink streaming job that answers both questions.

---

## Environment Setup

### 1. Install Java and Python tools

Update the system:

```bash
sudo apt update
sudo apt install -y openjdk-11-jdk python3 python3-venv python3-pip
```

Check Java:

```bash
java -version
```

**Screenshot:** Capture the terminal showing the installed Java version.

### 2. Create the project

```bash
mkdir -p windowed-aggregations
cd windowed-aggregations

python3 -m venv venv
source venv/bin/activate
```

Install PyFlink:

```bash
pip install apache-flink
```

Check the installation:

```bash
python -c "import pyflink; print('PyFlink installed successfully')"
```

**Screenshot:** Capture the terminal showing `PyFlink installed successfully`.

---

# Chapter 1 — Create the Event Stream

A streaming job starts with events. Each event contains a `user_id`, an event type, and an event timestamp.

### 1.1 What You Will Build

The event format will be:

```text
(user_id, event_type, timestamp)
```

Example:

```text
("user_101", "click", 10:01)
```


### 1.3 Create `window_lab.py`

Create the file:

```bash
nano window_lab.py
```

**Screenshot:** Capture the `window_lab.py` file showing the event data.

Add the following code:

```python
from datetime import datetime

events = [
    ("user_101", "click", "2026-10-04 10:01:00"),
    ("user_101", "login", "2026-10-04 10:02:00"),
    ("user_102", "click", "2026-10-04 10:03:00"),
    ("user_101", "purchase", "2026-10-04 10:04:00"),

    ("user_102", "login", "2026-10-04 10:06:00"),
    ("user_101", "click", "2026-10-04 10:07:00"),
    ("user_102", "click", "2026-10-04 10:08:00"),

    ("user_101", "purchase", "2026-10-04 10:11:00"),
    ("user_102", "click", "2026-10-04 10:12:00"),
]
```

The timestamps are intentionally spread across several minutes so that the window results are easy to observe.

---

# Chapter 2 — Build the Tumbling Window

A **tumbling window** divides time into fixed, non-overlapping intervals.

For a 5-minute window:

```text
10:00 ───── 10:05
10:05 ───── 10:10
10:10 ───── 10:15
```

An event belongs to only one window.


### 2.2 Implementation

Add the following imports and helper function to `window_lab.py`:

```python
from pyflink.common import Types
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import (
    TimestampAssigner,
    WatermarkStrategy,
)
from pyflink.datastream import (
    StreamExecutionEnvironment,
)
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import (
    AggregateFunction,
    ProcessWindowFunction,
)
```

Create the event-time timestamp assigner:

```python
class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return int(
            datetime.strptime(
                value[2], "%Y-%m-%d %H:%M:%S"
            ).timestamp() * 1000
        )
```

Create a simple count aggregation:

```python
class CountAggregate(AggregateFunction):
    def create_accumulator(self):
        return 0

    def add(self, value, accumulator):
        return accumulator + 1

    def get_result(self, accumulator):
        return accumulator

    def merge(self, a, b):
        return a + b
```

Create the Flink environment:

```python
env = StreamExecutionEnvironment.get_execution_environment()

event_stream = env.from_collection(
    events,
    type_info=Types.TUPLE([
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
    ])
)

watermark_strategy = (
    WatermarkStrategy
    .for_monotonous_timestamps()
    .with_timestamp_assigner(EventTimestampAssigner())
)

timestamped_stream = event_stream.assign_timestamps_and_watermarks(
    watermark_strategy
)
```

Now create the 5-minute tumbling window:

```python
tumbling_result = (
    timestamped_stream
    .key_by(lambda x: x[0], key_type=Types.STRING())
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .aggregate(CountAggregate())
)

tumbling_result.print()

env.execute("5-Minute Tumbling Window")
```

### 2.3 Understanding the Code

The important flow is:

```text
Events
  ↓
Assign event timestamps
  ↓
key_by(user_id)
  ↓
5-minute tumbling window
  ↓
Count events
  ↓
Print result
```

The key operation is:

```python
.key_by(lambda x: x[0], key_type=Types.STRING())
```

Here, `x[0]` is the `user_id`.


---

# Chapter 3 — Build the Sliding Window

A **sliding window** moves forward by a fixed slide interval and can overlap with previous windows.

For a 10-minute window that slides every 5 minutes:

```text
10:00 ───────── 10:10
       10:05 ───────── 10:15
```

The windows overlap.


### 3.1 Implementation

Replace the tumbling window section with the following:

```python
from pyflink.datastream.window import SlidingEventTimeWindows
```

Create the sliding window:

```python
sliding_result = (
    timestamped_stream
    .key_by(lambda x: x[0], key_type=Types.STRING())
    .window(
        SlidingEventTimeWindows.of(
            Time.minutes(10),
            Time.minutes(5)
        )
    )
    .aggregate(CountAggregate())
)

sliding_result.print()

env.execute("10-Minute Sliding Window")
```

The important part is:

```python
SlidingEventTimeWindows.of(
    Time.minutes(10),
    Time.minutes(5)
)
```

This means:

```text
Window size = 10 minutes
Slide      = 5 minutes
```


---

# Chapter 4 — Compare the Results

The two window types answer different questions.

| Window | Size | Overlap | Use |
|---|---:|---|---|
| Tumbling | 5 minutes | No | Fixed-period counts |
| Sliding | 10 minutes | Yes | Moving/rolling counts |

### Experiment: Change the Slide

Change:

```python
Time.minutes(5)
```

to:

```python
Time.minutes(2)
```

**Screenshot:** Capture the changed slide interval in the code editor.

Run the program again.

**Screenshot:** Capture the new output after changing the slide interval.

**Observe:** The 10-minute window now moves every 2 minutes, so more overlapping windows are produced.

**Question:** What happens to the number of windows when the slide interval becomes smaller?


Restore the slide interval to:

```python
Time.minutes(5)
```

---


## Screenshot Checklist

Add one clear screenshot for each practical step:

1. **Environment Setup** — Java version.
2. **PyFlink Installation** — successful PyFlink import.
3. **Event Stream** — `window_lab.py` showing the sample events.
4. **Tumbling Window** — code and/or terminal output showing the 5-minute count.
5. **Sliding Window** — code and/or terminal output showing the 10-minute count.
6. **Experiment** — changed slide interval and the new output.

Keep screenshots focused on the relevant terminal or code area. Avoid unnecessary full-screen screenshots.


## Epilogue: The Complete System

The completed pipeline is:

```text
User Events
     |
     v
Event Timestamp
     |
     v
key_by(user_id)
     |
     +----------------------+
     |                      |
     v                      v
5-min Tumbling       10-min Sliding
     |                      |
     v                      v
Event Count             Event Count
     |                      |
     +----------+-----------+
                |
                v
             Results
```

The lab now calculates per-user event counts in both fixed and overlapping time windows.

### Final Verification

Confirm that you can:

```text
1. Create the event stream
2. Assign event timestamps
3. Group events by user_id
4. Apply a 5-minute tumbling window
5. Apply a 10-minute sliding window
6. Count events in each window
```

## The Principles

1. **Key the stream by the required entity** — Use `user_id` when results must be calculated per user.
2. **Choose the window based on the question** — Tumbling windows provide fixed-period summaries; sliding windows provide overlapping moving views.
3. **Use event time for time-based analysis** — Event timestamps determine which window receives an event.
4. **Windowing reduces an unbounded stream into useful summaries** — Each window produces a manageable result.

## Troubleshooting

### Error: `ModuleNotFoundError: No module named 'pyflink'`

Make sure the virtual environment is active:

```bash
source venv/bin/activate
```

Then install PyFlink:

```bash
pip install apache-flink
```

### Error: Java is not available

Check:

```bash
java -version
```

**Screenshot:** Capture the terminal showing the installed Java version.

If Java is missing:

```bash
sudo apt update
sudo apt install -y openjdk-11-jdk
```

### Error: PyFlink job fails during startup

Check the installed versions:

```bash
python --version
java -version
pip show apache-flink
```

Use a supported Python/Java combination for the installed PyFlink version.

## Next Steps

After completing this lab, extend the job by:

- counting different event types separately
- adding a second user attribute
- testing out-of-order events
- experimenting with different window sizes and slide intervals

## Conclusion

Windowed aggregation provides a practical way to summarize continuous event streams. In this lab, you grouped events by `user_id` and calculated counts using both a 5-minute tumbling window and a 10-minute sliding window.

The main distinction is that tumbling windows do not overlap, while sliding windows can overlap. This allows the same streaming data to support both fixed-period reporting and moving real-time analysis.
