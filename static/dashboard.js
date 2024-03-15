function filterQueues() {
    const input = document.getElementById('queue-search');
    const filter = input.value.toUpperCase();
    const dropdown = document.getElementById("search-dropdown");
    const list = document.getElementById('queue-list').getElementsByTagName('li');
  
    // Clear the dropdown
    dropdown.innerHTML = '';
    
    // Only display dropdown if there is a search term
    dropdown.style.display = input.value !== '' ? 'block' : 'none';
  
    // Loop through all list items and add those that match the search query to the dropdown
    for (const listItem of list) {
      const a = listItem.getElementsByTagName("a")[0];
      const txtValue = a.textContent || a.innerText;
      if (txtValue.toUpperCase().includes(filter)) {
        const matchingListItem = listItem.cloneNode(true);
        dropdown.appendChild(matchingListItem);
      }
    }
  }
  

async function postData(url = '', data = {}) {
    const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
    });
    if (!response.ok) {
        throw new Error(`HTTP error! Status: ${response.status}`);
    }
    return await response.json();
}

function displayResult(message, index) {
    document.querySelectorAll('.result-content')[index].textContent = message;
}

async function createQueue() {
    try {
        const queueName = document.getElementById('queue-name').value;
        const data = await postData('/create-queue', { queueName });
        displayResult('Queue created with ID: ' + data.queueId, 0);
    } catch (error) {
        displayResult('Error creating queue: ' + error.message, 0);
    }
}

async function enqueueMessage() {
    try {
        const queueName = document.getElementById('enqueue-queue-name').value;
        const messageBody = document.getElementById('message-body').value;
        const data = await postData('/enqueue-message', { queueName, messageBody });
        displayResult('Message enqueued with ID: ' + data.messageId, 1);
    } catch (error) {
        displayResult('Error enqueuing message: ' + error.message, 1);
    }
}

async function dequeueMessage() {
    try {
        const queueName = document.getElementById('dequeue-queue-name').value;
        const data = await postData('/dequeue-message', { queueName });
        displayResult('Dequeued message: ' + data.messageBody, 2);
    } catch (error) {
        displayResult('Error dequeuing message: ' + error.message, 2);
    }
}

async function getQueueAttributes() {
    try {
        const queueName = document.getElementById('attributes-queue-name').value;
        // Encode the queue name to ensure special characters are properly handled in the URL
        const encodedQueueName = encodeURIComponent(queueName);
        const response = await fetch(`/queue-attributes?queueName=${encodedQueueName}`);

        if (!response.ok) {
            throw new Error(`HTTP error! Status: ${response.status}`);
        }

        const data = await response.json();
        const attributes = `Queue ID: ${data.queueId}\nCreated at: ${new Date(data.created_at * 1000).toLocaleString()}\nAvailable Messages: ${data.availableMessages}\nConsumed Messages: ${data.consumedMessages}`;
        displayResult(attributes, 3);

    } catch (error) {
        displayResult('Error fetching queue attributes: ' + error.message, 3);
    }
}
