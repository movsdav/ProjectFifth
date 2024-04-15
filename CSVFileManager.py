import pandas as pd


class CSVFileManager:
    dim_claim_path = "./data_files/(Coterie) V_DimClaim.csv"
    dim_claimant_path = "./data_files/(Coterie) V_DimClaimant.csv"
    dim_policy_path = "./data_files/(Coterie) V_DimPolicy.csv"

    def __init__(self):
        # Create DataFrame for each .csv file using paths
        self.dim_claim_df = self.get_df_from_csv(self.dim_claim_path)
        self.dim_claimant_df = self.get_df_from_csv(self.dim_claimant_path)
        self.dim_policy_df = self.get_df_from_csv(self.dim_policy_path)

        # Get row count
        # We assume that all of 3 files have same row count
        self.row_count = len(self.dim_claim_df)

    def process_data(self, count_of_rows=100, new_file_name="result.csv"):
        # A list where we store data from columns
        result_list = []

        # Check the count of rows to be more than 0 and less or equal than maximum count of rows
        if count_of_rows > self.row_count or count_of_rows < 0:
            raise ValueError(f"We cant process more than {self.row_count} rows or less than 0.")

        # Iterate over file rows
        for i in range(count_of_rows):
            # Get row from DimClaim.csv file
            dim_claim_row = self.__get_dim_claim_row(i)
            # Get data from DimClaim.csv file
            data_from_claim_file = self.__get_dim_claim_info(dim_claim_row)

            # claim_id and policy_number variables used to get linked data in DimClaimant.csv and DimPolicy.csv files
            claim_id, policy_number, _ = data_from_claim_file

            # Get data from DimClaimant.csv file
            data_from_claimant_file = self.__get_dim_claimant_info(claim_id)
            # Get data from DimPolicy.csv file
            data_from_policy_file = self.__get_dim_policy_info(policy_number)

            # Push collected data to list
            result_list.append([*data_from_claim_file, *data_from_claimant_file, *data_from_policy_file])

        # Create pandas DataFrame object with collected list
        result_df = pd.DataFrame(result_list,
                                 columns=["CLAIMID", "POLICYNUMBER", "CURRENTCLAIMSTATUSGROUP", "CLAIMANTNAME",
                                          "POLICYCONTACTEMAIL",
                                          "POLICYCONTACTPHONENUMBER"])

        # Save result to csv file
        self.__save_to_file(new_file_name, result_df)

    @staticmethod
    def get_df_from_csv(file_name):
        return pd.read_csv(file_name)

    def __save_to_file(self, file_name, df):
        df.to_csv(file_name, index=False)

    # Method to get row from DimClaim.csv
    def __get_dim_claim_row(self, index):
        return self.dim_claim_df.iloc[index]

    # Method to get data from columns in DimClaim.csv file
    # In our case we get data only from CLAIMID, POLICYNUMBER, CURRENTCLAIMSTATUSGROUP columns
    def __get_dim_claim_info(self, dim_claim_row):
        claim_id = dim_claim_row["CLAIMID"]
        policy_number = dim_claim_row["POLICYNUMBER"]
        current_claim_status_group = dim_claim_row["CURRENTCLAIMSTATUSGROUP"]
        return claim_id, policy_number, current_claim_status_group

    # Method to get data from DimClaimant.csv
    # Only CLAIMANTNAME column data
    def __get_dim_claimant_info(self, claim_id):
        dim_claimant_row = self.dim_claimant_df[self.dim_claimant_df["CLAIMANTID"] == claim_id]
        claimant_name = "N/A"

        # Check if column empty
        if len(dim_claimant_row) > 0:
            claimant_name = dim_claimant_row["CLAIMANTNAME"].values[0]

        return (claimant_name,)

    # Method to get data from DimPolicy.csv file
    # Columns: POLICYCONTACTPHONENUMBER, POLICYCONTACTEMAIL
    def __get_dim_policy_info(self, policy_number):
        dim_policy_row = self.dim_policy_df[self.dim_policy_df["POLICYNUMBER"] == policy_number]
        policy_contact_phone_number = "N/A"
        policy_contact_email = "N/A"

        # Check if column empty
        if len(dim_policy_row) > 0:
            policy_contact_phone_number = dim_policy_row["POLICYCONTACTPHONENUMBER"].values[0]
            policy_contact_email = dim_policy_row["POLICYCONTACTEMAIL"].item()

        return policy_contact_phone_number, policy_contact_email
