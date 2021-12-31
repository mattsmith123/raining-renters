import { KinesisClient, DescribeStreamCommand,  GetShardIteratorCommand, GetRecordsCommand, Timestamp} from '@aws-sdk/client-kinesis'
import _ from 'lodash'
import dayjs from 'dayjs'

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}


export default class EventStream {
    constructor({streamName, streamAccessKeyId, streamSecretAccessKey, region}) {
        const credentials = {accessKeyId: streamAccessKeyId, secretAccessKey: streamSecretAccessKey}
        this.client = new KinesisClient({ credentials, region})
        this.streamName = streamName
        this.delay = 0
        this.identify = {}
        this.reservations = {}
        this.byRudderId = {}
        this.mergedRID = {}
        this.mergedUsers = {}
        this.users = {}
        this.byUserId = {}
        this.byEvent = { track:{}, identify:{}}
        this.all = []
        this.currData
        this.lastTimestamp = dayjs().subtract(1, 'day')
        window.stream = this
    }

    getLastTimestamp() {
        return this.lastTimestamp
    }
    async initialize(timestamp) {
        console.log("init")
        const streamProps = await this.client.send(new DescribeStreamCommand({
            StreamName: this.streamName
        }))
        this.shardIterator = (await this.client.send(new GetShardIteratorCommand({
            StreamName: this.streamName,
            ShardId: streamProps.StreamDescription.Shards[0].ShardId,
            ShardIteratorType: 'AT_TIMESTAMP',
            Timestamp: timestamp
        }))).ShardIterator
    }

    filterAsOf(asOfTime, sessionTimeoutMillis) {
        const endTime = asOfTime.subtract(sessionTimeoutMillis, 'ms')
        return (u) => (u.searchInfo && u.firstUpdate.isBefore(asOfTime) && u.lastUpdate.isAfter(endTime)) || u.reservations
    }

    async readAsOf(asOfTime, sessionTimeoutMillis) {
        const unmerged = _.filter( Object.values(this.users), (u) => !(u.id in this.mergedRID))
        const merged = _.map(this.mergedUsers, (mu) => mu.user)
        return _.chain(unmerged.concat(merged))
            .filter(this.filterAsOf(asOfTime, sessionTimeoutMillis))
            .sortBy( (u) => u.firstUpdate)
            .value()
    }

    async read(delay) {
        if (this.delay) {
            console.log("waiting", this.delay)
            await sleep(this.delay)
        }
        return this.client.send(new GetRecordsCommand({
            ShardIterator: this.shardIterator
        }))
            .catch ( (e) => {
                console.log(e)
                if (!this.delay) {
                    this.delay = 1000
                } else {
                    this.delay = this.delay * 1.5
                }
                return this.read(delay)
            })
            .then( (data) => {
                this.delay = this.delay * .9
//                console.log('read', data)
                this.shardIterator = data.NextShardIterator
                const parsedRecords = data.Records.map( (d) => {
                    const s = String.fromCharCode(...d.Data)
                    d.parsedData = JSON.parse(s)
                    return d
                })
                this.applyEvents(parsedRecords)

                return parsedRecords
            })
    }

    safeDictAppend(dict, key, value) {
        if (!dict[key]) {
            dict[key] = []
        }
        dict[key].push(value)
    }

    mergeUsers(currUser, message) {
        const event = message.parsedData
        const newUserEvents = this.byRudderId[event.rudderId]
        this.mergedRID[event.rudderId] = null
        currUser.usersMerged[event.rudderId] = newUserEvents
        const allEvents = _.chain(currUser.usersMerged)
            .flatMap()
            .sortBy( (m) => m.SequenceNumber)
            .value()
        return allEvents.reduce(this.reduceUser, {})
    }
    
    applyEvents(events) {
        events.forEach( (message) => {
            const event = message.parsedData
            this.all.push(message)
            this.lastTimestamp = dayjs(event.timestamp)
            this.safeDictAppend(this.byRudderId, event.rudderId, message)
            this.safeDictAppend(this.byEvent[event.type], event.event || "non", message)
            if (event.userId) {
                this.safeDictAppend(this.byUserId, event.userId, message)
            }
            const curr = this.users[event.rudderId] || {}
            this.users[event.rudderId] = this.reduceUser(curr, message)
            if (event.type == 'identify') {
                if (event.userId) {
                    if (!this.mergedUsers[event.userId]){
                        this.mergedUsers[event.userId] = {
                            usersMerged: {}
                        }
                    }
                    const user = this.mergedUsers[event.userId]
                    if (!user.usersMerged[event.rudderId]) {
                        const newUser = this.mergeUsers(user, message)
                        user.user = newUser
                    } else  {
                        user.user = this.reduceUser(user.user, message)
                    }
                }
            }
            if (event.properties?.reservationId) {
                this.safeDictAppend(this.reservations, event.properties.reservationId, message)
            }
        })
    }

    // maybe rxjs would work better?
    reduceUser(prev, curr) {
        const event = curr.parsedData
        if (!prev.firstUpdate) {
            prev.firstUpdate = dayjs(event.timestamp)
        }
        prev.lastUpdate = dayjs(event.timestamp)
        prev.id = event.rudderId
        prev.cnt = (prev.cnt || 0) + 1
        prev.debug = prev.debug || {}
        prev.debug.events = prev.debug.events || []
        prev.debug.events.push(curr)
        prev.debug.eventNames = prev.debug.eventNames || []
        prev.debug.eventNames.push(curr.parsedData.event || curr.parsedData.type)
        if (event.userId) {prev.userId = event.userId}
        if (event.event == 'complete search') {
            prev.searchInfo = prev.searchInfo || {
                    searchPath: []
                }
            const searchInfo = prev.searchInfo
            searchInfo.lastSearch = {
                lat: event.properties.lat,
                lng: event.properties.lng
            }
            searchInfo.searchPath.push(searchInfo.lastSearch)
        }
        if (event.properties?.reservationId) {
            const reservationId = event.properties?.reservationId
            prev.reservations = prev.reservations || {}
            prev.reservations[reservationId] = prev.reservations[reservationId] || {}
            if (event.event == 'complete reservation request') {
                prev.reservations[reservationId].requestTime = dayjs(event.timestamp)
            }
            if (event.event == 'receive reservation request approve') {
                prev.reservations[reservationId].approvalTime = dayjs(event.timestamp)
            }

        }
        const traits = event?.context?.traits
        if (traits) {
            prev.traits = prev.traits || {}
            Object.assign(prev.traits, traits)
        }
        if (event.eventType == 'identify' && event.properties) {
            prev.traits = prev.traits || {}
            Object.assign(prev.traits, event.properties)
        }
        [prev.lat, prev.lng] = prev.searchInfo ?
            [prev.searchInfo.lastSearch.lat, prev.searchInfo.lastSearch.lng] :
            [prev.traits?.Latitude, prev.traits?.Longitude]
        prev.consolidatedId = prev.userId || prev.id
        return prev
    }

    async prime(timestamp) {
        return this.initialize(timestamp).then( async () => {
            let records = await this.read()
            let zerocount = 0
            while (zerocount < 3) {
//                console.log(records)
                records = await this.read()
                if (records.length == 0) {
                    zerocount = zerocount + 1
                } else {
                    zerocount = 0
                }
            }
            return this.byEvent
        })
    }
}