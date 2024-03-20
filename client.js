function range(start, stop, step) {
  if (typeof stop == 'undefined') {
      // one param defined
      stop = start;
      start = 0;
  }

  if (typeof step == 'undefined') {
      step = 1;
  }

  if ((step > 0 && start >= stop) || (step < 0 && start <= stop)) {
      return [];
  }

  var result = [];
  for (var i = start; step > 0 ? i < stop : i > stop; i += step) {
      result.push(i);
  }

  return result;
};

// Store for all of the jobs in progress
let jobs = {};

// Kick off a new job by POST-ing to the server
async function addJob() {
  const date = document.getElementById('job-date').value
  if (!date.match(/^\d{4}-\d{2}-\d{2}$/)) {
    alert("Invalid date");
    return;
  }
  let res = await fetch(`job/${date}`, {method: 'POST'});
  let job = await res.json();
  jobs[job.id] = {id: job.id, state: "queued"};
  render();
}

async function seeJobs() {
  from = parseInt(document.getElementById('job-start').value)
  to = parseInt(document.getElementById('job-end').value);
  jobs = Object.fromEntries(range(from, to).map( x => [x.toString(), {
      id: x.toString(),
      state: "Updating state..."
    }]) 
  );
}

// Fetch updates for each job
async function updateJobs() {
  for (let id of Object.keys(jobs)) {
    let res = await fetch(`/job/${id}`);
    let result = await res.json();
    if (!!jobs[id]) {
      jobs[id] = result;
    }
    render();
  }
}

// Delete all stored jobs
function clear() {
  jobs = {};
  render();
}

// Update the UI
function render() {
  let s = "";
  for (let id of Object.keys(jobs)) {
    s += renderJob(jobs[id]);
  }

  // For demo simplicity this blows away all of the existing HTML and replaces it,
  // which is very inefficient. In a production app a library like React or Vue should
  // handle this work
  document.querySelector("#job-summary").innerHTML = s;
}

// Renders the HTML for each job object
function renderJob(job) {
  let progress = job.progress || 0;
  let color = "bg-light-purple";

  if (job.state === "completed") {
    color = "bg-purple";
    progress = 100;
  } else if (job.state === "failed") {
    color = "bg-dark-red";
    progress = 100;
  }
  
  return document.querySelector('#job-template')
    .innerHTML
    .replace('{{id}}', job.id)
    .replace('{{date}}', job.data?.date ?? 'No date')
    .replace('{{state}}', job.state)
    .replace('{{reason}}', job.reason ?? '')
    .replace('{{color}}', color)
    .replace('{{progress}}', progress);
}

// Attach click handlers and kick off background processes
window.onload = function() {
  document.querySelector("#add-job").addEventListener("click", addJob);
  document.querySelector("#see-jobs").addEventListener("click", seeJobs);
  document.querySelector("#clear").addEventListener("click", clear);

  setInterval(updateJobs, 1000);
};
