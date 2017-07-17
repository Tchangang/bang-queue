# Bang-queue

Bang is a mongoDB queue (FIFO) designed for NodeJS.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Installing

Install bang-queue from npm

```
npm install bang-queue
```

### Use queue

Create a new Bang object

```
const Bang = new Bang('your mongo uri connection','queue name here')
```

### Creating jobs
Create a new job

jobtype : Type of job
arguments : data that will be send when job will occured. Ex : {created:new Date}
params : job configuration object. {timeout:50000,delay:5000}

Default timeout : 50000
Default delay : 0

```
Bang.createJob(jobtype,arguments,params)
.then((result)=>{
	res.json({result})
})
.catch((e)=>{
	res.json({e})
})
```

### Listening event

Start listening for new job

```
const Bang = new Bang('your mongo uri connection','queue name here')
```

Bang.on('job_type',10,(err,data)=>{
	if(err){
		console.log(err)
	}else if(job){
		// process your job here
		// job._id
		// job.arguments
	}
})

### Complete and delete job

Complete and delete a job. Job not deleted will stay in queue and will be repush in the queue.
You should delete completed job. 

```
Bang.setCompleteJob(job._id,{delete:true})
.then((result)=>{
	// Job deleted
})
.catch((e)=>{
	// catch error here
})
```

## Authors

* **Tchangang Boris-Emmanuel** - *Initial work*

<!-- See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project. -->

## License

This project is licensed under the MIT License

