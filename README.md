# POSPipelineGerman

Implementation of a Spark NLP pipeline to extract PoS-Tags and Lemmas in german texts. The pipeline was mainly build to analyse articles from the News-Corpus that was 
created in this project: https://github.com/I-News-Pipeline-HTW-Berlin

## DAO

Currently news articles can be loaded from MongoDB or File. More implementations can be added to dao package.

## Pipeline

The current Spark NLP pipeline (`PosPipeline`) is analysing articles for PoS-Tags and Lemmas. Extend PipelineTrait to create new pipelines.

## utils

The `Conversion` object is used to change structure of DataFrames, for example after articles have been loaded from MongoDB. It's usage is optional.

## Runners

Runners should have the suffix `App` and can be added to package runners. 

## Build

To create an executable `.jar` file with dependencies `sbt-assembly` plugin is used. `sbt clean assembly` has to be run in project directory. 

## Run

To run the application as spark application use `spark-submit` command. An example call for local mode could be `spark-submit --packages com.johnsnowlabs.nlp:spark-nlp_2.11:2.6.2 --class runners.PosPipelineApp --driver-memory 7g --master local[*] POSPipelineGerman-assembly-0.1.jar`
