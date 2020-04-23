# serverless-sim
A simulator for serverless platforms (mostly the scheduler part, simulating Openwhisk's [ShardingContainerPoolBalancer](https://github.com/apache/openwhisk/blob/master/core/controller/src/main/scala/org/apache/openwhisk/core/loadBalancer/ShardingContainerPoolBalancer.scala))

Usage: `./run.py < run.json`

Where `run.json` includes the parameters.

Results will be saved in `runs/<digest>.pdf`
