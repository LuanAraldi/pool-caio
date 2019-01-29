const RateLimiter = require('request-rate-limiter')
const request = require('request-promise-native')
const fs = require('fs')

const TOKEN = ''

const PROJECT_URL = 'https://app.asana.com/api/1.0/projects/680563298779433/tasks?opt_fields=assignee,completed,completed_at,created_at,custom_fields,id,memberships,name,notes,parent,projects,subtasks,tags'
const TASK_URL = 'https://app.asana.com/api/1.0/tasks/'
const TAGS_URL = 'https://app.asana.com/api/1.0/tags/'

const limiter = new RateLimiter({
    rate: 1000,
    interval: 70,
    backoffCode: 429
})

let data = []
let promissePool = []

process.on('exit', (code) => {
    fs.writeFileSync("/home/luan/workspace/pool-caio/output/asana_data.json", JSON.stringify(data))
    console.log('Asana file saved!')
});

function getSubtasks (dataPool) {
    dataPool.forEach((task, taskIndex) => {
        if (task.subtasks) {
            task.subtasks.forEach((subtask, subtaskIndex) => {
                limiter.request().then((backoff) => {
                    console.log(promissePool.length)
                    promissePool.push(request({
                        method: 'GET',
                        url: TASK_URL + subtask.id,
                        resolveWithFullResponse: true,
                        headers: {
                            'Content-Type': 'application/json',
                            'Authorization': TOKEN
                        }
                    }).then((response) => {
                        if (response.statusCode === 429) {
                            isBackedOff = true
                            console.log('back off')
                            backoff()
                            console.log('backed off')
                        } else {
                            data[taskIndex].subtasks[subtaskIndex] = JSON.parse(response.body).data
                        }
                    }).catch((err) => {
                        console.log(err)
                    }))
                })
            }, this)
        }
    }, this)
}

function getTags (dataPool) {
    dataPool.forEach((task, taskIndex) => {
        if (task.subtasks) {
            task.tags.forEach((tag, tagIndex) => {
                limiter.request().then((backoff) => {
                    console.log(promissePool.length)
                    promissePool.push(request({
                        method: 'GET',
                        url: TAGS_URL + tag.id,
                        resolveWithFullResponse: true,
                        headers: {
                            'Content-Type': 'application/json',
                            'Authorization': TOKEN
                        }
                    }).then((response) => {
                        if (response.statusCode === 429) {
                            isBackedOff = true
                            console.log('back off')
                            backoff()
                            console.log('backed off')
                        } else {
                            data[taskIndex].tags[tagIndex] = JSON.parse(response.body).data
                        }
                    }).catch((err) => {
                        console.log(err)
                    }))
                })
            }, this)
        }
    }, this)
}

const retrieveProjectInformation = (limit, offset) => {
    limiter.request({
        method: 'GET',
        url: `${PROJECT_URL}&limit=${limit}&offset=${offset}`,
        headers: {
            'Content-Type': 'application/json',
            'Authorization': TOKEN
        }
    }, (err, response) => {
        console.log(err)
        const next_page = JSON.parse(response.body).next_page
    
        data = data.concat(JSON.parse(response.body).data)
        console.log(data.length)
    
        if (next_page) {
            retrieveProjectInformation(100, next_page.offset)
        } else {
            getSubtasks(data)
            getTags(data)
        }
    })
}

limiter.request({
    method: 'GET',
    url: `${PROJECT_URL}&limit=100`,
    headers: {
        'Content-Type': 'application/json',
        'Authorization': TOKEN
    }
}, (err, response) => {
    console.log(err)
    const next_page = JSON.parse(response.body).next_page
    
    data = JSON.parse(response.body).data
    console.log(next_page)
    if (next_page) {
        retrieveProjectInformation(100, next_page.offset)
    }
});
