var lastTimestamp = Math.floor(Date.now() / 1000) - 86400; // Default to now minus 14400 seconds
var newTimestamp = lastTimestamp * 1;
//console.log('lastTimestamp:', lastTimestamp);

const queryString = window.location.search;
//console.log(queryString);
const urlParams = new URLSearchParams(queryString);
const from_timestamp = urlParams.get('from_timestamp')
//console.log('from_timestamp:', from_timestamp);

const max_items = urlParams.get('max_items')
console.log('max_items:', max_items);
const namespaces = urlParams.get('namespaces')
console.log('namespaces:', namespaces);

var ts_array = [];
function fetchDataAndUpdateTable() {
  var host = window.location.origin;
  var fetch_url = host + "/api?smoke=true&from_timestamp=" + lastTimestamp;
  console.log('fetch_url (lastTimestamp):', fetch_url);
  if (from_timestamp > lastTimestamp) {
      fetch_url = host + "/api?smoke=true&from_timestamp=" + from_timestamp;
      console.log('fetch_url (from_timestamp):', fetch_url);
  }
  if (max_items) {
    fetch_url = fetch_url + "&max_items=" + max_items;
    console.log('fetch_url (max_items):', fetch_url);
  }
  if (namespaces) {
    fetch_url = fetch_url + "&namespaces=" + namespaces;
    console.log('fetch_url (namespaces):', fetch_url);
  }

  let i = 0;
  while (i < ts_array.length) {
      i++;
      lastTimestamp = ts_array[i];
      break;
  }
  var image_url = host + '/ionosphere_images?image='; 
  fetch(fetch_url)
      .then(response => response.json())
      .then(data => {
//        console.log('data:', data);
//        console.log(Object.keys(data));
//        console.log('data.data.training_data:', data.data.training_data);

          ts_array = [];
          const table = document.getElementById('data-table');

          // Show the spinner
          const spinner = document.getElementById('loading-spinner');

          for (const t1 in data.data.training_data) {
              ts_array.push(t1);
          }
          if (ts_array.length > 0) {
            // Remove or hide the spinner
            spinner.style.display = 'none'; // or table.removeChild(spinner);
          }
//          console.log('ts_array:', ts_array);
          const reverse_ts_array = ts_array.reverse();
//          console.log('reverse_ts_array:', reverse_ts_array);
//          for (const ts in data.data.training_data.reverse()) {
          let i = 0;
          while (i < reverse_ts_array.length) {
              const ts2 = reverse_ts_array[i];
              i++;
//              console.log('match ts2:', ts2);
              for (const ts in data.data.training_data) {
//                  console.log('ts:', ts);
                  if (ts===ts2) {
//                      console.log('ts:', ts);
                      const ts_item = data.data.training_data[ts];
//                      console.log('ts_item:', ts_item);
                      for (const metric in ts_item) {
                          if (metric === "thunder_alert") {
                            const item = ts_item[metric];
//                            console.log('thunder item:', item);
                            metrics_array = [];                  
                            for (const stale_metric in item.metrics) {
                                metrics_array.push(item.metrics[stale_metric]);
                                if (item.metrics.length === metrics_array.length) {
                                    break;
                                }
                                if (metrics_array.length > 9) {
                                    break;
                                }
                            }
//                            console.log('thunder metrics_array:', metrics_array);
                            let metrics_str = '';
                            // metrics_array.forEach(item => metrics_str += '<br>');                    
                            let i = 0;
                            while (i < metrics_array.length) {
                                metrics_str += '<code>' + metrics_array[i] + '</code><br>';
                                i++;
                            }
                            metrics_str += '</code><br>';              
//                            console.log('thunder metrics_str:', metrics_str);
                            if (item.status === 'recovered') {
                                message_status = '<font color="green"><strong>' + item.status.toUpperCase() + '</strong></font>';
                            } else if (item.status === 'not recieving data for some metrics') {
                                message_status = '<font color="red"><strong>' + item.status.toUpperCase() + '</strong></font>';
                            } else {
                                message_status = '<strong>' + item.status.toUpperCase() + '</strong>';
                            }
                            message = `${item.date} - <strong>THUNDER</strong> - ${item.metrics.length} stale metrics ${message_status} <a target='_blank' href="${item.url}">(link to full event)</a> (sample of max 10)<br>${metrics_str}`;
                            // Create table row and cells
                            const row = table.insertRow();
                            const cell1 = row.insertCell(0);
                            // Set message and image
                            cell1.innerHTML = message;
                            continue;
                          }

                          const item = ts_item[metric];

// TODO
//                          const thunder = item.thunder;

//                          console.log('item:', item);
                          lastTimestamp = item.details.until;
                          if (lastTimestamp > newTimestamp) {
//                            console.log('newTimestamp:', newTimestamp);
                            newTimestamp = lastTimestamp * 1;
//                            console.log('new newTimestamp:', newTimestamp);
                          }
                          const lower_app = item.details.added_by;
                          const app = lower_app.toUpperCase();
                          const metric_name = item.details.metric;
//                          console.log('metric_name:', metric_name);
                          const base_name = item.details.base_name;
//                          console.log('base_name:', base_name);

                          const training_data_uri = item.training_data_uri
                          if (metric_name === base_name) {
                              message = `${item.date} - <strong>${app}</strong> - <a target='_blank' href="${training_data_uri}">${base_name}</a> was ${item.details.value}<br>`;
                          } else {
                              message = `${item.date} - <strong>${app}</strong> - <code>${base_name}</code> (<a target='_blank' href="${training_data_uri}">${metric_name}</a>) was ${item.details.value}<br>`;
                          }
                          const imageUrl = image_url + item.image;
                          const trained = item.trained;
                          const matched = item.matched;
                          const untrainable = item.untrainable;

                          const trainButtonUrl = item.training_data_uri + "&api_train_request=true&add_fp=true&learn=false&label=none&format=json";

                          // Create table row and cells
                          const row = table.insertRow();
                          const cell1 = row.insertCell(0);
//                          const cell2 = row.insertCell(1);

                          if (matched !== true && item.image == null) {
                            message = `${message}No graph created yet.  Open the <a target='_blank' href="${training_data_uri}">training_data</a> page to view graphs<br>`;
                          }
                          // Set message and image
                          cell1.innerHTML = message;

//                  console.log('message:', message);
//                          if (matched !== true) {
                          if (matched !== true && item.image != null) {
                            const image = document.createElement('img');
                            image.loading = 'lazy';
                            image.src = imageUrl;
                            image.width = 580; // Set the width of the image
                            image.height = 260; // Set the height of the image
    //                          cell2.appendChild(image);
                            cell1.appendChild(image);
                          }
          //                  console.log('imageUrl:', imageUrl);
          //              break; // Break after processing the first (most recent) item
                          // Create and append a button
                          const button = document.createElement('a'); // Using an anchor tag as a button
                          if (matched === true) {
                            button.href = training_data_uri;  // Set the URL for the button
                            button.target = '_blank'; // Open in a new tab
                            button.textContent = 'Matched'; // Text displayed on the button
                            button.className = 'btn btn-success'; // Add Bootstrap button classes for styling  
                          } else {
                            if (trained === true) {
                              button.href = training_data_uri;  // Set the URL for the button
                              button.target = '_blank'; // Open in a new tab
                              button.textContent = 'Already trained'; // Text displayed on the button
                              button.className = 'btn btn-success'; // Add Bootstrap button classes for styling
                            } else if (untrainable === true) {
                              button.href = training_data_uri;  // Set the URL for the button
                              button.target = '_blank'; // Open in a new tab
                              button.textContent = 'Insufficient date to train'; // Text displayed on the button
                              button.className = 'btn btn-warning'; // Add Bootstrap button classes for styling
                            } else {
                              if (matched !== true && item.image != null) {
                                button.href = trainButtonUrl;  // Set the URL for the button
                                button.target = '_blank'; // Open in a new tab
                                button.textContent = 'Train (as normal)'; // Text displayed on the button
                                button.className = 'btn btn-primary'; // Add Bootstrap button classes for styling
                              }
                            }
                          }
                          cell1.appendChild(button); // Append the button to the cell

                      }
                  }
              }
          }
      })
      .catch(error => console.error('Error fetching data:', error));
//  console.log('lastTimestamp:', lastTimestamp);
//  console.log('newTimestamp:', newTimestamp);
      
}

//console.log('lastTimestamp:', lastTimestamp);
//console.log('newTimestamp:', newTimestamp);

// Call the function immediately on page load
fetchDataAndUpdateTable();

//console.log('lastTimestamp:', lastTimestamp);
//console.log('newTimestamp:', newTimestamp);

// Call the function every 60 seconds
//setInterval(fetchDataAndUpdateTable, 60000);