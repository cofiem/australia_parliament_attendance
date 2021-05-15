from datetime import datetime, timedelta

from attendance.main import Main

main = Main()
start_date = datetime.now() - timedelta(weeks=1)
end_date = datetime.now()
main.get_page_data(start_date, end_date)
