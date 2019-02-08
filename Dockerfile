FROM coady/pylucene
RUN pip install tweepy
RUN pip install django
CMD ["python", "TwCollector.py"]
