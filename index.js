'use strict'

let mongodb = require('mongodb')
let ObjectId = mongodb.ObjectID

const EventEmitter = require('events')

const Bang = function(mongoUri,queueName,params){	
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
	this.DEBUG = false

	if(params){
		if(params.REFRESH_DELAY)
			this.REFRESH_DELAY = params.REFRESH_DELAY
		if(params.DEFAULT_TIMEOUT)
			this.DEFAULT_TIMEOUT = params.DEFAULT_TIMEOUT
		if(params.DEBUG)
			this.DEBUG = params.DEBUG
	}

	mongodb.MongoClient.connect(this.MONGO_URI, (err, database)=>{
	  	if(err){
	  		throw err
	  		// console.log('ERROR')
	  		// console.log(err)
	  	}else{
	  		// console.log('connected')
	  		this.isInit = true	
	  		this.mongo = database
	  		this.cursor = {}
	  		this.cursor.jobs = database.collection('bang_jobs_'+queueName)
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
						// console.log('Inside promote - job not found ',_id)
						reject(new Error('Job not found'))
					}
					const jobFound = result
					this.cursor.jobs.update({_id:ObjectId(_id)},{$set:{promotedAt:new Date().getTime(),expireAt:new Date().getTime()+jobFound.timeout}},(err,result)=>{
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
			// console.log('Params in setCompleteJob : '+_id+' - '+params)
			if(_id){
				this.cursor.jobs.findOneAndUpdate(
				{_id:ObjectId(_id),state:1},
				{$set:{state:0,completedAt:new Date().getTime()}}, 
				{returnOriginal:false},
				(err,result)=>{
					if(err){
						reject(err)
					}else{
						if(result && result.value){
							let jobFound = result.value
							if(params && params.delete){
								this.cursor.jobs.remove({_id:ObjectId(_id)},(err,result)=>{
									if(err){
										reject(err)
									}else{
										this.eventList[jobFound.typeText].inProgress--
										resolve({statut:1,deletedAt:new Date()})
									}
								})
							}else{
								this.eventList[jobFound.typeText].inProgress--
								resolve({statut:1,updatedAt:new Date()})
							}
						}else{
							resolve({statut:1,updateAt:new Date(0)})
						}
					}
				})
				// this.cursor.jobs.findOne({_id:ObjectId(_id)},(err,result)=>{
				// 	if(err){
				// 		reject(err)
				// 	}
				// 	// console.log(result)
				// 	if(!result){
				// 		reject(new Error('Job not found with _id '+_id))
				// 	}else{
				// 		const jobFound = result
				// 		if(jobFound){
				// 			this.cursor.jobs.update({_id:ObjectId(_id)},({$set:{state:0,completedAt:new Date().getTime()}}),(err,result)=>{
				// 				if(err){
				// 					reject(err)
				// 				}
				// 				if(params && params.delete){
				// 					this.cursor.jobs.remove({_id:ObjectId(_id)},(err,result)=>{
				// 						if(err){
				// 							reject(err)
				// 						}else{
				// 							this.eventList[jobFound.typeText].inProgress--
				// 							resolve({statut:1,deletedAt:new Date()})
				// 						}
				// 					})
				// 				}else{
				// 					this.eventList[jobFound.typeText].inProgress--
				// 					resolve({statut:1,updatedAt:new Date()})
				// 				}
				// 			})
				// 		}
				// 	}
				// })
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
			if(this.eventList[type].inProgress<this.eventList[type].max){
				this.cursor.jobs.findOneAndUpdate(
				{type:this.hashQueueName(type),queueName:this.QUEUE_NAME_HASH,startAt:{$lt:new Date().getTime()},state:-1},
				{$set:{state:1}}, 
				{sort:{createdAt:1},returnOriginal:false},
				(err,result)=>{
					if(err){
						reject(err)
					}else{
						if(result)
							result.key = type
						else
							result = {value:null,key:type}
						resolve(result)
					}
				})
			}else{
				resolve({value:null,key:type})
			}
		})
	}

	this.updateExpiredJob = ()=>{
		return new Promise((resolve, reject) => {
			this.cursor.jobs.updateMany({expireAt:{$lt:new Date().getTime()},state:1,queueName:this.QUEUE_NAME_HASH,retry:{$lt:this.MAX_RETRY}},{$set:{state:-1,expireAt:new Date().getTime()+this.DEFAULT_TIMEOUT},$inc:{retry:1}},(err,result)=>{
				if(err){
					reject(err)
				}
				// if(result && result.result && result.result.nModified>0){
				// 	console.log('New Update expire')
				// }
				this.cursor.jobs.updateMany({expireAt:{$lt:new Date().getTime()},queueName:this.QUEUE_NAME_HASH,state:1,retry:{$gt:this.MAX_RETRY-1}},{$set:{state:-2}},(err,result)=>{
					if(err){
						reject(err)
					}
					// if(result && result.result.nModified>0){
					// 	console.log('New Update expire')
					// }
					resolve({statut:1,updatedExpiredJob:new Date()})
				})
			})
		})
	}

	this.requeueJob = (_id)=>{
		return new Promise((resolve, reject) => {
			this.cursor.jobs.findOne({_id:ObjectId(_id)},(err,jobFound)=>{
				if(err){
					reject(err)
				}
				if(jobFound && jobFound.state != -1  && jobFound.state != -2){
					// On va continuer
					if(jobFound.retry<this.MAX_RETRY){
						let timeout = this.DEFAULT_TIMEOUT
						if(jobFound.timeout){
							timeout = jobFound.timeout
						}
						this.cursor.jobs.update({_id:ObjectId(_id)},{$set:{state:-1,timeout,expireAt:new Date().getTime()+timeout},$inc:{retry:1}},(err,result)=>{
							if(err){
								reject(err)
							}
							resolve(result)
						})
					}else{
						this.cursor.jobs.update({_id:ObjectId(_id)},{$set:{state:-2}},(err,result)=>{
							if(err){
								reject(err)
							}
							resolve(result)
						})
					}
				}else{
					reject(new Error('Job not found'))
				}
			})
		})
	}

	this.createJob = (type,_arguments,params)=>{
		return new Promise((resolve, reject) => {
			let toInsert = {type:this.hashQueueName(type),arguments:_arguments,typeText:type,queueName:this.QUEUE_NAME_HASH,createdAt:new Date(),state:-1,retry:0}
			if(params.timeout){
				toInsert.expireAt = 9999999999999
				toInsert.timeout = params.timeout
			}else{
				toInsert.expireAt = 9999999999999
				toInsert.timeout = this.DEFAULT_TIMEOUT
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
				resolve(result.ops[0]._id)
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
				this.eventList[eventType].inProgress++
				// console.log('*************************')
				// console.log('Date from event',data)
				// console.log('*************************')
				this.setPromoteJob(data._id)
				.then((result)=>{
					const done = (error)=>{
						return new Promise((resolve, reject) => {
							// console.log('data in done function',data)
							if(error){
								// Ici on va remettre le job dans la queue
								this.requeueJob(data._id)
								.then((result)=>{
									resolve({statut:1,requeueAt:new Date().getTime()})
								})
								.catch((e)=>{
									reject(e)
								})
							}else{
								this.setCompleteJob(data._id,{delete:true})
								.then((result)=>{
									resolve({statut:1})
								})
								.catch((e)=>{
									reject(e)
								})
							}	
						})
					}
					callback(null,data,done)
				})
				.catch((e)=>{
					callback(e,null,null)
				})
			})
		}else{
			callback(new Error('No type event found'),null,null)
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
				  		if(result && result.value!=null && result.key){
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
					// console.log(e)
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

const serialQueue = function(mongoUri,queueName){
	if(!mongoUri || !queueName){
		throw new Error('Please init queue properly please')
	}
	this.MONGO_URI = mongoUri
	this.QUEUE_NAME = queueName
	this.mongo = null
	this.cursor = null
	this.isInit = false

	mongodb.MongoClient.connect(this.MONGO_URI, (err, database)=>{
	  	if(err){
	  		// console.log('Error while connecting to MongoDB',err)
	  		throw err
	  	}else{
	  		// console.log('Mongo connected')
	  		this.isInit = true	
	  		this.mongo = database
	  		this.cursor = {}
	  		this.cursor.queues = database.collection('bang_serial_'+this.QUEUE_NAME)
	  	}
	})

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

	this.setDelay = (key,delay)=>{
		return new Promise((resolve, reject) => {
			if(this.isInit){
				const hashKey = this.hashQueueName(key)
				this.cursor.queues.findOne({key:hashKey},(err,item)=>{
					if(err){
						reject(err)
					}
					let lastSeen = new Date().getTime()+delay

					if(item){
						lastSeen = item.lastSeen
						if(!lastSeen){
							lastSeen = new Date().getTime()+delay
						}else{
							lastSeen = lastSeen + delay
							if(lastSeen<new Date().getTime()){
								lastSeen = new Date().getTime()
							}
						}
					}
					let toInsert = {key:hashKey,keyText:key,createdAt:new Date(),lastSeen}
					this.cursor.queues.update({key:hashKey},toInsert,{upsert:true},(err,result)=>{
						if(err){
							reject(err)
						}
						resolve({delay:lastSeen-new Date().getTime(),dateTime:lastSeen})
					})
				})	
			}else{
				reject(new Error('Mongo not yet init. Wait please'))
			}
		})
	}
}

exports.serialQueue = serialQueue
exports.Bang = Bang