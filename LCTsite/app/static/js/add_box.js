


async function verifyAbbreviation(questionNumber) {
    // This part now checks if the user's input for questions 1 and 4 needs to be verified against the database
    const userInput = document.getElementById(`q${questionNumber}`).value;

    // Send an AJAX request to the back-end to check if the input exists in the database
    const response = await fetch("/verify_abbreviation", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ userInput, questionNumber }),
    });

    const responseData = await response.json();
    console.log(responseData);

    if (responseData.exists) {
        // The input exists in the database, so proceed to the next question
        console.log(
            `User input for question ${questionNumber} exists in the database.`
        );
        return true;
    } else {
        // The input does not exist in the database, so ask for additional input (e.g., name)
        const assxName = prompt(`What name does this abbreviation stand for?`);
        const abbrvInsertStatus = await insertAbbrv(
            userInput,
            assxName,
            questionNumber
        );

        // If needed, you can display a message or take any other action based on the finalResponse
        console.log(abbrvInsertStatus);

        return true;
    }
}

async function insertAbbrv(input, assxName, qNum) {
    // Send an AJAX request to the back-end to insert the data into the database
    const response = await fetch("/insert_abbreviation", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ input, assxName, qNum }),
    });
    return await response.json();
}

async function validations(currentQuestionNumber) {
    const userInput = document.getElementById(`q${currentQuestionNumber}`).value;
    // check for if abbreviation exists
    if (currentQuestionNumber === 1 || currentQuestionNumber === 4) {
        if (userInput.trim() === "") {
            return true;
        } else {
            const validAbbreviation = await verifyAbbreviation(currentQuestionNumber);
    
            console.log(validAbbreviation)
    
            if (validAbbreviation) {
                return true;
            } else {
                return false;
            }
        }

    } else {
        return true;
    }

    /* 

    consider verifications:
        price format
        date continuity (dates cant be in future, start date cant be before purchase, finish cant be before start)

    */

}

async function handleInput(currentQuestionNumber) {
    const validInput = await validations(currentQuestionNumber);
    console.log(validInput)

    if (validInput) {
        const currentQuestion = document.getElementById(
            `question${currentQuestionNumber}`
        );
        currentQuestion.style.display = "none";

        const nextQuestion = document.getElementById(
            `question${currentQuestionNumber + 1}`
        );
        nextQuestion.style.display = "block";
        console.log(`Showing question ${currentQuestionNumber + 1}`);
    } else {
        // const errorMessage = document.querySelector(`#question${currentQuestionNumber} .add-data-error-message`)
        // console.log(errorMessage)
        // errorMessage.style.display = "inline";
        return;
    }
}

async function finalSubmission() {
    // iterate through all questions and obtain values
    // AJAX req with values to complete box data insertion
    // toggle success message display to show (block)
    event.preventDefault();

    const formData = {
        'dataType': 'box'
    };

    for (let questionNumber = 1; questionNumber <= 6; questionNumber++) {
        const inputField = document.getElementById(`q${questionNumber}`);
        const inputValue = inputField.value;

        formData[`q${questionNumber}`] = inputValue;
    }

    try {
        const response = await fetch("/submit_form", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(formData),
        });

        console.log(response)

        if (response.ok) {
            document.getElementById("questions-form").style.display = "none";
            const successDialog = document.getElementById("success-modal");

            successDialog.showModal();

            setTimeout(function () {
                successDialog.close(); // Hide the popup after a short delay
                window.location.href = "/add_data"; // Redirect to /add_data after the popup disappears
            }, 1); // Popup display time (adjust as needed)
        } else {
            console.error("Error submitting the form");
        }
    } catch (error) {
        console.error("Error submitting the form:", error);
    }
}

document
    .getElementById("questions-form")
    .addEventListener("submit", finalSubmission);



