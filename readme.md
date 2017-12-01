# Silhouette (clustering measure) support for KNIME

Implements the [Silhouette algorithm](https://en.wikipedia.org/wiki/Silhouette_%28clustering%29) (clustering score) for
the squared Euclidean (sum of squares) measure.

## Limitations

Currently only squared Euclidean measure is supported and if the column
with labels specified, the labels have to be compatible with the PMML
model.
