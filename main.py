from CSVFileManager import CSVFileManager
from flask import Flask, request, render_template

app = Flask(__name__)
csv_file_manager = CSVFileManager()


@app.route("/", methods=["GET"])
def index():
    return render_template("index.jinja")


@app.route("/result", methods=["POST"])
def result():
    # Get the number of rows that has to be mixed
    number_of_rows = int(request.form["countOfRows"])
    # Get the new file name
    result_file_name = request.form["resultFileName"]

    file_name = f"./data_files/{result_file_name}.csv"

    # Mix the files
    csv_file_manager.process_data(number_of_rows, file_name)

    # Get DataFrame from new file
    result_df = CSVFileManager.get_df_from_csv(file_name)

    # Render page
    return render_template("result.jinja", data=result_df.to_html(index=True))


if __name__ == "__main__":
    app.run(port=7777, debug=True)
