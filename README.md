Simple project that was sparked out of idea to compare potential performance and drawbacks of several ways to
calculate UUID5 in PySpark as there is no apparent default implementation.

There are several versions implemented here, note that not all of them are similar and equally stable, especially
PyArrow which was written with significant ChatGPT assistance in guiding to study specific APIs over the course
of less than couple of hours.

Performance on different datasets can vary substantially as well. Very rough estimation is that Pandas (or pure PySpark if it's actually on par)
approach is still the most advantageous as being the easiest to read and maintain.

Most of the codebase was created by Brend Braeckmans and Danny Meijer.

PyArrow (by wrangling with ChatGPT and docs) and pure PySpark mimic of UUID5 (by just reverse implementing its Python
function + ChatGPT assistance) are added as an idea mostly.

`data_generator.py` was written to generate the exemplar dataset (10k lorem-ipsum-like rows).
