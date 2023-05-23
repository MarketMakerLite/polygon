"""
To use this script, you must first download the ykman tools from https://developers.yubico.com/yubikey-manager/
"""
import subprocess
import getpass
import sys
import getpass
import pwinput


# import yubikey_manager


def remove_key(label=None):
    if not label:
        label = input("Enter the label of the key to be removed: ")

    print(f"Removing key '{label}' from Yubikey device...")
    subprocess.run(["ykman", "otp", "password", "remove", label])
    print("Key removed successfully.")


def create_key():
    label = input("Enter a label for the secret to be stored: ")

    secret = pwinput.pwinput(prompt='Enter the secret to store: ', mask='*')  # Change the prompt.

    # Store the password on the YubiKey with the label
    subprocess.run(["ykman", "otp", "password", "--label", label, secret])

    # Retrieve the password from the YubiKey
    validate_value = subprocess.run(["ykman", "otp", "password", "--label", label], stdout=subprocess.PIPE)

    # Decode the binary output of the subprocess call into a string, and then strip any leading/trailing whitespace
    if secret == validate_value.stdout.decode().strip():
        print("Key created successfully.")
    else:
        print("The stored secret did not match the specified secret.")
        remove_key(label)


if __name__ == '__main__':
    # Loop until the user gives a proper response or chooses to exit
    while True:
        # Prompt the user for their choice
        choice = input("Would you like to 'create', 'remove', or 'exit'? ")

        # Call the appropriate function based on the user's choice
        if choice == "create":
            create_key()
        elif choice == "remove":
            remove_key()
        elif choice == "exit":
            print("Exiting.")
            break
        else:
            # Remind the user of the options
            print("Invalid response. Please choose 'create', 'remove', or 'exit'.")
