# zeebe-db-reader

Example how to read element instances, jobs and workflows from a Zeebe state.

## Build

```
mvn clean package
```

## Read Workflow

```
java -cp target/zeebe-db-reader-1.0-SNAPSHOT-jar-with-dependencies.jar org.menski.zdb.WorkflowDb PATH_TO_DB WORKFLOW_KEY
```

## Read Element Instance

```
java -cp target/zeebe-db-reader-1.0-SNAPSHOT-jar-with-dependencies.jar org.menski.zdb.ElementInstanceDb PATH_TO_DB ELEMENT_INSTANCE_KEY
```

## Read Job

```
java -cp target/zeebe-db-reader-1.0-SNAPSHOT-jar-with-dependencies.jar org.menski.zdb.JobsDb PATH_TO_DB JOB_KEY
```

