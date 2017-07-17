'use strict'

let mongodb = require('mongodb')
let ObjectId = mongodb.ObjectID

const EventEmitter = require('events')

const Bang = function(mongoUri,queueName){	
	this.MONGO_URI = mongoUri
	this.QUEUE_NAME = queueName
	this.MAX_RETRY = 5
	this.emitter = new EventEmitter.EventEmitter()
	this.eventList = {}
	this.isInit = false
	this.mongo = null
	this.cursor = null
	this.DEFAULT_TIMEOUT = 30000
	this.REFRESH_DELAY = 200
	mongodb.MongoClient.connect(this.MONGO_URI, (err, database)=>{
	  	if(err){
	  		throw err
	  		console.log('ERROR')
	  		console.log(err)
	  	}else{
	  		console.log('connected')
	  		this.isInit = true	
	  		this.mongo = database
	  		this.cursor = {}
	  		this.cursor.queues = database.collection('bang_queues')
	  		this.cursor.jobs = database.collection('bang_jobs')
	  		this.cursor.params = database.collection('bang_running')
	  		// lancement de la fonction de polling
			this.emitter.emit('bang_poll') 	
	  	}
	})	

	this.getJob = (_id)=>{
		return new Promise((resolve, reject) => {
			if(type){
				this.cursor.jobs.findOne({_id:ObjectId(_id)},(err,result)=>{
					if(err){
						reject(err)
					}
					resolve(result)
				})
			}else{
				reject(new Error('Type message not defined'))
			}
		})
	}	

	this.setPromoteJob = (_id)=>{
		return new Promise((resolve, reject) => {
			if(_id){
				this.cursor.jobs.findOne({_id:ObjectId(_id)},(err,result)=>{
					if(err){
						reject(err)
					}
					if(!result){
						reject(new Error('Job not found'))
					}
					const jobFound = result
					this.cursor.jobs.update({_id:ObjectId(_id)},{$set:{promotedAt:new Date().getTime()}},(err,result)=>{
						if(err){
							reject(err)
						}
						resolve({statut:1,promotedAt:new Date()})
					})
				})
			}else{
				reject(new Error('Type message not defined'))
			}
		})
	}

	this.setCompleteJob = (_id,params)=>{
		return new Promise((resolve, reject) => {
			if(_id){
				this.cursor.jobs.findOne({_id:ObjectId(_id)},(err,result)=>{
					if(err){
						reject(err)
					}
					if(!result){
						reject(new Error('Job not found'))
					}
					const jobFound = result
					this.cursor.jobs.update({_id:ObjectId(_id)},({$set:{state:0,completedAt:new Date().getTime()}}),(err,result)=>{
						if(err){
							reject(err)
						}
						if(params && params.delete){
							this.cursor.jobs.remove({_id:ObjectId(_id)},(err,result)=>{
								if(err){
									reject(err)
								}
								this.eventList[jobFound.typeText].inProgress--
								resolve({statut:1,deletedAt:new Date()})
							})
						}else{
							resolve({statut:1,updatedAt:new Date()})
						}
					})
				})
			}else{
				reject(new Error('Type message not defined'))
			}
		})
	}

	this.hashQueueName = function(str) {
	  	let hash = 0, i, chr;
	  	if (str.length === 0) return hash;
	  	for (i = 0; i < str.length; i++) {
	    	chr   = str.charCodeAt(i);
	    	hash  = ((hash << 5) - hash) + chr;
	    	hash |= 0; // Convert to 32bit integer
	  	}
	  	return hash
	}
	this.QUEUE_NAME_HASH = this.hashQueueName(queueName)

	this.getNextJob = (type)=>{
		return new Promise((resolve, reject) => {
			//console.log({type:this.hashQueueName(type),expireAt:{$lt:new Date().getTime()},state:-1})
			this.cursor.jobs.updateOne({type:this.hashQueueName(type),queueName:this.QUEUE_NAME_HASH,startAt:{$lt:new Date().getTime()},state:-1},{$set:{state:1}},{sort:{createdAt:1}},(err,result)=>{
				if(err){
					reject(err)
				}
				if(result.result && result.result.nModified>0){
					this.cursor.jobs.findOne({type:this.hashQueueName(type),queueName:this.QUEUE_NAME_HASH,startAt:{$lt:new Date().getTime()},state:1},{sort:{createdAt:1}},(err,result)=>{
						if(err){
							reject(err)
						}
						resolve({value:result,ok:1,key:type})
					})
				}else{
					resolve({value:null,ok:1,key:type})
				}
			})
		})
	}

	this.updateExpiredJob = ()=>{
		return new Promise((resolve, reject) => {
			this.cursor.jobs.updateMany({expireAt:{$lt:new Date().getTime()},state:1,queueName:this.QUEUE_NAME_HASH,retry:{$lt:this.MAX_RETRY}},{$set:{state:-1,expireAt:new Date().getTime()+this.DEFAULT_TIMEOUT},$inc:{retry:1}},(err,result)=>{
				if(err){
					reject(err)
				}
				if(result && result.result && result.result.nModified>0){
					console.log('New Update expire')
				}
				this.cursor.jobs.updateMany({expireAt:{$lt:new Date().getTime()},queueName:this.QUEUE_NAME_HASH,state:1,retry:{$gt:this.MAX_RETRY}},{$set:{state:-2}},(err,result)=>{
					if(err){
						reject(err)
					}
					if(result && result.result.nModified>0){
						console.log('New Update expire')
					}
					resolve({statut:1,updatedExpiredJob:new Date()})
				})
			})
		})
	}

	this.createJob = (type,_arguments,params)=>{
		return new Promise((resolve, reject) => {
			let toInsert = {type:this.hashQueueName(type),arguments:_arguments,typeText:type,queueName:this.QUEUE_NAME_HASH,createdAt:new Date(),state:-1,retry:0}
			if(params.timeout){
				toInsert.expireAt = new Date().getTime()+params.timeout
			}else{
				toInsert.expireAt = new Date().getTime()+this.DEFAULT_TIMEOUT
			}
			if(params.delay){
				toInsert.startAt = new Date().getTime()+params.delay
			}else{
				toInsert.startAt = new Date().getTime()-1
			}
			this.cursor.jobs.insert(toInsert,(err,result)=>{
				if(err){
					reject(err)
				}
				resolve({statut:1})
			})
		})
	}

  	this.on = (eventType,maxParallels,callback)=>{
		if(eventType && maxParallels && typeof eventType==='string' && typeof maxParallels==='number'){
			if(!this.eventList[eventType]){
				this.eventList[eventType] = {}
				this.eventList[eventType].max = maxParallels
				this.eventList[eventType].inProgress = 0
			}
			// Ici on sauvegarde le nombre de taches en cours
			this.emitter.on(eventType,(data)=>{
				if(this.eventList[eventType].inProgress<this.eventList[eventType].max){
					this.eventList[eventType].inProgress++
					this.setPromoteJob(data._id)
					.then((result)=>{
						callback(null,data)
					})
					.catch((e)=>{
						callback(e,null)
					})
				}else{
					callback(new Error('Queue busy for event '+eventType+' - '+this.eventList[eventType].inProgress,null))
				}
			})
		}else{
			callback(new Error('No type event found'),null)
		}
	}

	// ******************************************
	// Configuration du polling 
	// ******************************************
	this.nextPoll = ()=>{
		setTimeout(()=>{
			this.emitter.emit('bang_poll')
		}, this.REFRESH_DELAY)
	}

	this.emitter.on('bang_poll',()=>{
		if(this.mongo){
			let tabPromise = []
			for(let key in this.eventList){
				tabPromise.push(this.getNextJob(key))
			}
			tabPromise.push(this.updateExpiredJob())
			if(tabPromise.length>1){
				Promise.all(tabPromise)
				.then((values)=>{ 
				  	for(let result of values){
				  		if(result.value!=null && result.key){
				  			const toSend = {
				  				_id:result.value._id,
				  				arguments:result.value.arguments
				  			}
				  			this.emitter.emit(result.key,toSend)		
				  		}
				  	}
				  	this.nextPoll()
				})
				.catch((e)=>{
					console.log(e)
					this.nextPoll()
				})
			}else{
				this.nextPoll()
			}
		}else{
			this.nextPoll()
		}
	})
}

exports.Bang = Bang