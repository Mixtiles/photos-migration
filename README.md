# Photo Migration

This repository contains the code that migrates photos from Cloudindary and Filestack.

It is run though its own [Heroku pipeline](https://dashboard.heroku.com/pipelines/69610998-55d9-4bba-840c-5a8046b92ba1).

## Getting Started

1. `npm install`
2. `npm start`
3. [http://localhost:5000](http://localhost:5000)

## Application Overview

### Processes

The application is comprised of 2 process: 

- **`web.js`** - An [Express](https://expressjs.com/) server that serves the frontend assets, accepts new background jobs, and reports on the status us existing jobs
- **`worker.js`** - A small node process that listens for and executes incoming jobs

Because these are separate processes, they can be scaled independently based on specific application needs. Read the [Process Model](https://devcenter.heroku.com/articles/process-model) article for a more in-depth understanding of Heroku’s process model.

The `web` process serves the `index.html` and `client.js` files which implement a simplified example of a frontend interface that kicks off new jobs and checks in on them.

### Jobs

The application is comprised of 1 job (runs thorugh Heroku Scheduler):

- **`create_new_jobs.js`** - Create new jobs as needed, assuming we are migration from the
future to the past.
