# Hymir

Hymir is a python library for creating and running simple workflows by composing
together Chains (which run sequentially) and Groups (which run in parallel) of
jobs.

Hymir is built on top of [redis][] for intermediate storage and tracking
workflow state, and comes with an Executor for running the workflows using
[Celery][].

## Installation

```bash
pip install hymir
```

## Usage

See the latest documentation and examples at https://tkte.ch/hymir/.
