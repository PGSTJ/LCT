


async function formatData() {
    // formats user submitted can data by box
    const boxID = document.getElementById('q1').value;
    const canData = document.getElementById('q2');

    const allCans = [];
    
    for (let i = 1; i <= 8; i++) {
        const can = [];
        const row = canData.rows[i];
        const inputs = row.querySelectorAll("input");
        
        for (const input of inputs) {
            can.push(input.value);
        }
        allCans.push(can);
    }


    const formData = {
        'dataType': 'can',
        'boxID': boxID,
        'canData': allCans
    };

    return formData;

}

async function submission(formData) {
    // input processing and redirect
    try {
        const response = await fetch("/submit_form", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(formData),
        });

        if (response.ok) {
            document.getElementById("questions-form").style.display = "none";


            setTimeout(function () {
                window.location.href = "/add_data"; // Redirect to /add_data after the popup disappears
            }, 1); // Popup display time (adjust as needed)
        } else {
            console.error("Error submitting the form");
        }
    } catch (error) {
        console.error("Error submitting the form:", error);
    }
}



async function uploadCans() {
    event.preventDefault();

    const data = await formatData();

    return await submission(data);
}
    

document
    .getElementById("questions-form")
    .addEventListener("submit", finalSubmission);