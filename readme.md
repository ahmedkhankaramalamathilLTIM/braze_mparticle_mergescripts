
#1 USE chunkify.py to split the csv file into chunks
usage #python chunkify.py


#2 USE step1_email_mobile_identifier_child_profiles.py to modify the child profiles (Email/Mobile)
usage #python step1_email_mobile_identifier_child_profiles.py

#3 USE step2_email_identifier_winner_profile.py to modify the winner profiles (Email)
usage #python step2_email_identifier_winner_profile.py

#3 USE step2_mobile_identifier_winner_profile.py to modify the winner profiles (Mobile)
usage #python step2_mobile_identifier_winner_profile.py



Make sure to change the environment to production and DRY_RUN to False before running the scripts
Make sure to change the input directory to the chunked csv files
Make sure to change the output chunk directory to the chunked csv files
Make sure to change the log file name to the chunked csv files
