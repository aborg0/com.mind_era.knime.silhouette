package com.mind_era.knime.silhouette;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.knime.base.node.mine.cluster.PMMLClusterTranslator;
import org.knime.base.node.mine.cluster.PMMLClusterTranslator.ComparisonMeasure;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.NominalValue;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelColumnName;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.pmml.PMMLPortObject;
import org.knime.core.node.streamable.DataTableRowInput;
import org.knime.core.node.streamable.InputPortRole;
import org.knime.core.node.streamable.MergeOperator;
import org.knime.core.node.streamable.OutputPortRole;
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.streamable.PortInput;
import org.knime.core.node.streamable.PortObjectInput;
import org.knime.core.node.streamable.PortOutput;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.streamable.RowOutput;
import org.knime.core.node.streamable.StreamableOperator;
import org.knime.core.node.streamable.StreamableOperatorInternals;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.pmml.PMMLModelType;
import org.knime.core.util.Pair;
import org.w3c.dom.Node;

/**
 * This is the model implementation of FastSilhouette. Allows computing the
 * squared Euclidean <a href=
 * "https://en.wikipedia.org/wiki/Silhouette_(clustering)">Silhouette</a> (score
 * of a clustering) as a distributed algorithm.
 *
 * @author Mind Eratosthenes Kft.
 */
class FastSilhouetteNodeModel extends NodeModel {
	private static final String MEASURE = "measure";
	private static final String SQUARED_EUCLIDEAN = "squared Euclidean";
	private static final String COSINE = "cosine";
	private static final String OPTIONAL_LABEL_COLUMN = "labelOpt";

	protected static final Set<String> SUPPORTED_MEASURES = Collections.unmodifiableSet(
			new LinkedHashSet<>(Arrays.asList(SQUARED_EUCLIDEAN/* , COSINE */)));

	static final int MODEL_INDEX = 0;
	static final int DATA_INDEX = 1;

	private final SettingsModelString measure = createMeasureSettings();

	private final SettingsModelColumnName labelOpt = createOptionalLabelColumnSettings();

	/**
	 * Constructor for the node model.
	 */
	protected FastSilhouetteNodeModel() {
		super(new PortType[] { PMMLPortObject.TYPE, BufferedDataTable.TYPE }, new PortType[] {
				BufferedDataTable.TYPE/* , PMMLPortObject.TYPE */ });
	}

	/**
	 * @return
	 */
	protected static SettingsModelString createMeasureSettings() {
		return new SettingsModelString(MEASURE, SQUARED_EUCLIDEAN);
	}

	/**
	 * @return
	 */
	protected static SettingsModelColumnName createOptionalLabelColumnSettings() {
		return new SettingsModelColumnName(OPTIONAL_LABEL_COLUMN, null);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {
		exec.checkCanceled();
		// final BufferedDataContainer tempTable =
		// exec.createDataContainer(resultTableSpec());
		final BufferedDataTable data = (BufferedDataTable) inData[DATA_INDEX];
		final Pair<Map<String, Pair<ClusterStats, double[]>>, SortedMap<String, Integer>> clusterStatsAndColIndices = clusterStats(
				new DataTableRowInput(data), new PortObjectInput(inData[MODEL_INDEX]), exec);
		final Map<String, ClusterStats> clusterStats = extractClusterStats(clusterStatsAndColIndices);
		final Map<String, double[]> clusterCenters = extractClusterCenters(clusterStatsAndColIndices);
		final SortedMap<String, Integer> colIndices = clusterStatsAndColIndices.getSecond();
		// RowOutput rowOutput = new BufferedDataTableRowOutput(tempTable);
		// computeSilhouettes(stats, new DataTableRowInput(data), rowOutput,
		// exec);
		double silhouette = computeSilhouetteAverage(clusterStats, new DataTableRowInput(data), colIndices,
				clusterCenters, data.getSpec().findColumnIndex(labelOpt.getColumnName()), exec);
		final BufferedDataContainer container = exec.createDataContainer(resultTableSpec());
		container.addRowToTable(new DefaultRow(RowKey.createRowKey(0L), new DoubleCell(silhouette)));
		container.close();
		// Unfortunately it is not possible to add the silhouette stats with the
		// translator :(
		// PMMLClusterTranslator pmmlClusterTranslator = new
		// PMMLClusterTranslator();
		// PMMLPortObject pmmlPortObject = (PMMLPortObject)inData[MODEL_INDEX];
		// pmmlPortObject.initializeModelTranslator(pmmlClusterTranslator);
		// pmmlClusterTranslator.
		return new PortObject[] { container.getTable() };
	}

	/**
	 * @param clusterStatsAndColIndices
	 * @return
	 */
	private Map<String, double[]> extractClusterCenters(
			final Pair<Map<String, Pair<ClusterStats, double[]>>, SortedMap<String, Integer>> clusterStatsAndColIndices) {
		return clusterStatsAndColIndices.getFirst().entrySet().stream()
				.map(e -> new AbstractMap.SimpleImmutableEntry<>(e.getKey(), e.getValue().getSecond()))
				.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
	}

	/**
	 * @param clusterStatsAndColIndices
	 * @return
	 */
	private Map<String, ClusterStats> extractClusterStats(
			final Pair<Map<String, Pair<ClusterStats, double[]>>, SortedMap<String, Integer>> clusterStatsAndColIndices) {
		return clusterStatsAndColIndices.getFirst().entrySet().stream()
				.map(e -> new AbstractMap.SimpleImmutableEntry<>(e.getKey(), e.getValue().getFirst()))
				.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
	}

	/**
	 * @return
	 */
	private DataTableSpec resultTableSpec() {
		return new DataTableSpec(new DataColumnSpecCreator("Silhouette", DoubleCell.TYPE).createSpec());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void reset() {
		// No internal state.
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
		return new PortObjectSpec[] { resultTableSpec() };
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) {
		measure.saveSettingsTo(settings);
		labelOpt.saveSettingsTo(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		measure.loadSettingsFrom(settings);
		labelOpt.loadSettingsFrom(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		final SettingsModelString tmp = createMeasureSettings();
		tmp.loadSettingsFrom(settings);
		CheckUtils.checkSetting(SUPPORTED_MEASURES.contains(tmp.getStringValue()),
				"Not supported measure type: " + tmp.getStringValue());
		final SettingsModelColumnName tmp2 = createOptionalLabelColumnSettings();
		tmp2.loadSettingsFrom(settings);
		CheckUtils.checkSetting(tmp2.isEnabled() && !tmp2.useRowID(), "Wrong label settings");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadInternals(final File internDir, final ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
		// No internal state
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveInternals(final File internDir, final ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
		// No internal state
	}

	private static final class ClusterStats {
		private final double[] y;
		private double psi;
		private long n;

		/**
		 * @param y
		 * @param psi
		 * @param n
		 */
		public ClusterStats(double[] y, double psi, long n) {
			super();
			this.y = y;
			this.psi = psi;
			this.n = n;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return String.format("ClusterStats [y=%s, psi=%s, n=%s]", Arrays.toString(y), psi, n);
		}

	}

	/**
	 * Public only for the testing, so not use in other places!
	 */
	public static final class StreamableInternal extends StreamableOperatorInternals {
		private enum State {
			COLLECTING_CLUSTER_STATS, COMPUTING_SILHOUETTE_VALUES/* , AGGREGATING_SILHOUETTE_VALUES */;
		}

		/* labelOpt -> stats */
		private final Map<String, ClusterStats> statistics = new HashMap<>();

		private Optional<Double> result;
		private State state;
		private double silhouetteSumInPartition = 0d;
		private long rowCountInPartition = 0L;

		public StreamableInternal() {
			this(State.COLLECTING_CLUSTER_STATS, Optional.empty());
		}

		/**
		 * @param state
		 * @param result
		 */
		public StreamableInternal(final State state, final Optional<Double> result) {
			super();
			this.state = state;
			this.result = result;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.knime.core.node.streamable.StreamableOperatorInternals#load(java.
		 * io.DataInputStream)
		 */
		@Override
		public void load(DataInputStream input) throws IOException {
			try {
				state = State.values()[input.readInt()];
				final boolean allComputed = input.readBoolean();
				result = allComputed ? Optional.of(input.readDouble()) : Optional.empty();
				final int mapSize = input.readInt();
				final int len = input.readInt();
				statistics.clear();
				for (int i = mapSize; i-- > 0;) {
					final double[] ys = new double[len];
					final String key = input.readUTF();
					final long n = input.readLong();
					final double psi = input.readDouble();
					for (int j = 0; j < len; ++j) {
						ys[j] = input.readDouble();
					}
					statistics.put(key, new ClusterStats(ys, psi, n));
				}
				silhouetteSumInPartition = input.readDouble();
				rowCountInPartition = input.readLong();
			} finally {
				input.close();
			}
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.knime.core.node.streamable.StreamableOperatorInternals#save(java.
		 * io.DataOutputStream)
		 */
		@Override
		public void save(DataOutputStream output) throws IOException {
			try {
				output.writeInt(state.ordinal());
				output.writeBoolean(result.isPresent());
				result.ifPresent(d -> {
					try {
						output.writeDouble(d);
					} catch (IOException e) {
						throw new UncheckedIOException(e);
					}
				});
				// CheckUtils.checkState(!statistics.isEmpty(), "No statistics
				// are available!");
				output.writeInt(statistics.size());
				final int len = statistics.isEmpty() ? 0 : statistics.values().iterator().next().y.length;
				CheckUtils.checkState(statistics.values().stream().mapToInt(s -> s.y.length).allMatch(l -> l == len),
						"Wrong length for one of the y values");
				output.writeInt(len);
				for (final Entry<String, ClusterStats> entry : statistics.entrySet()) {
					output.writeUTF(entry.getKey());
					output.writeLong(entry.getValue().n);
					output.writeDouble(entry.getValue().psi);
					for (int i = 0; i < entry.getValue().y.length; i++) {
						final double d = entry.getValue().y[i];
						output.writeDouble(d);
					}
				}
				output.writeDouble(silhouetteSumInPartition);
				output.writeLong(rowCountInPartition);
			} finally {
				output.close();
			}
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.knime.core.node.NodeModel#createInitialStreamableOperatorInternals()
	 */
	@Override
	public StreamableInternal createInitialStreamableOperatorInternals() {
		return new StreamableInternal(FastSilhouetteNodeModel.StreamableInternal.State.COLLECTING_CLUSTER_STATS,
				Optional.empty());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.knime.core.node.NodeModel#createMergeOperator()
	 */
	@Override
	public MergeOperator createMergeOperator() {
		return new MergeOperator() {

			/*
			 * (non-Javadoc)
			 * 
			 * @see
			 * org.knime.core.node.streamable.MergeOperator#isHierarchical()
			 */
			@Override
			public boolean isHierarchical() {
				// As it it is both associative and commutative.
				return true;
			}

			@Override
			public StreamableInternal mergeFinal(StreamableOperatorInternals[] operators) {
				final StreamableInternal intermediate = mergeIntermediate(operators);
				final StreamableInternal ret = new StreamableInternal(
						StreamableInternal.State.COMPUTING_SILHOUETTE_VALUES,
						intermediate.rowCountInPartition != 0L
								? Optional.of(intermediate.silhouetteSumInPartition / intermediate.rowCountInPartition)
								: Optional.empty());
				ret.statistics.putAll(intermediate.statistics);
				return ret;
			}

			/*
			 * (non-Javadoc)
			 * 
			 * @see
			 * org.knime.core.node.streamable.MergeOperator#mergeIntermediate(
			 * org.knime.core.node.streamable.StreamableOperatorInternals[])
			 */
			@Override
			public StreamableInternal mergeIntermediate(StreamableOperatorInternals[] operators) {
				final Map<String, ClusterStats> stats = new HashMap<>();
				double silhouetteSum = 0d;
				long rowCount = 0L;
				for (final StreamableOperatorInternals internals : operators) {
					final StreamableInternal casted = (StreamableInternal) internals;
					for (final Entry<String, ClusterStats> entry : casted.statistics.entrySet()) {
						ClusterStats acc = stats.get(entry.getKey());
						if (acc == null) {
							acc = entry.getValue();
						} else {
							double[] y = acc.y;
							double[] other = entry.getValue().y;
							for (int i = y.length; i-- > 0;) {
								y[i] += other[i];
							}
							acc = new ClusterStats(y, acc.psi + entry.getValue().psi, entry.getValue().n + acc.n);
						}
						stats.put(entry.getKey(), acc);
					}
					silhouetteSum += casted.silhouetteSumInPartition;
					rowCount += casted.rowCountInPartition;
				}
				final StreamableInternal ret = createInitialStreamableOperatorInternals();
				ret.statistics.clear();
				ret.statistics.putAll(stats);
				ret.silhouetteSumInPartition = silhouetteSum;
				ret.rowCountInPartition = rowCount;
				ret.state = StreamableInternal.State.COMPUTING_SILHOUETTE_VALUES;
				if (rowCount > 0L) {
					ret.result = Optional.of(silhouetteSum / rowCount);
				}
				return ret;
			}
		};
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.knime.core.node.NodeModel#createStreamableOperator(org.knime.core.
	 * node.streamable.PartitionInfo, org.knime.core.node.port.PortObjectSpec[])
	 */
	@Override
	public StreamableOperator createStreamableOperator(PartitionInfo partitionInfo, PortObjectSpec[] inSpecs)
			throws InvalidSettingsException {
		return new StreamableOperator() {
			private StreamableInternal.State state;
			private final Map<String, ClusterStats> clusterStats = new TreeMap<>();
			private double silhouetteSum = 0d;
			private long rowCount = 0l;

			/*
			 * (non-Javadoc)
			 * 
			 * @see
			 * org.knime.core.node.streamable.StreamableOperator#runIntermediate
			 * (org.knime.core.node.streamable.PortInput[],
			 * org.knime.core.node.ExecutionContext)
			 */
			@Override
			public void runIntermediate(PortInput[] inputs, ExecutionContext exec) throws Exception {
				switch (state) {
				case COLLECTING_CLUSTER_STATS:
					final Pair<Map<String, Pair<ClusterStats, double[]>>, SortedMap<String, Integer>> clusterStatsAndColIndices = clusterStats(
							(RowInput) inputs[DATA_INDEX], (PortObjectInput) inputs[MODEL_INDEX], exec);
					clusterStats.clear();
					clusterStats.putAll(extractClusterStats(clusterStatsAndColIndices));
					state = StreamableInternal.State.COMPUTING_SILHOUETTE_VALUES;
					break;
				case COMPUTING_SILHOUETTE_VALUES:
					final RowInput data = (RowInput) inputs[DATA_INDEX];
					final PortObjectInput modelInput = (PortObjectInput) inputs[MODEL_INDEX];
					final PMMLClusterTranslator translator = new PMMLClusterTranslator();
					((PMMLPortObject) modelInput.getPortObject()).initializeModelTranslator(translator);
					final SortedSet<String> usedColumns = new TreeSet<>(translator.getUsedColumns());
					final SortedMap<String, Integer> colIndices = new TreeMap<>(usedColumns.stream()
							.collect(Collectors.toMap(k -> k, k -> data.getDataTableSpec().findColumnIndex(k))));
					int labelIdx = Optional.ofNullable(labelOpt.getColumnName())
							.map(col -> data.getDataTableSpec().findColumnIndex(col)).orElse(-1);
					if (labelOpt.getColumnName() != null) {

					}
					final double[] values = new double[colIndices.size()];
					DataRow row;
					while ((row = data.poll()) != null) {
						int i = 0;
						for (final Entry<String, Integer> e : colIndices.entrySet()) {
							final DataCell cell = row.getCell(e.getValue());
							values[i++] = ((DoubleValue) cell).getDoubleValue();
						}
						silhouetteSum += computeSilhouette(clusterStats, colIndices, labelIdx, values,
								translator.getLabels(), translator.getPrototypes(), row);
						rowCount++;
					}
					break;
				default:
					throw new IllegalStateException("Should not happen");
				}
			}

			/*
			 * (non-Javadoc)
			 * 
			 * @see
			 * org.knime.core.node.streamable.StreamableOperator#loadInternals(
			 * org.knime.core.node.streamable.StreamableOperatorInternals)
			 */
			@Override
			public void loadInternals(StreamableOperatorInternals internals) {
				final StreamableInternal casted = (StreamableInternal) internals;
				clusterStats.clear();
				clusterStats.putAll(casted.statistics);
				state = casted.state;
				silhouetteSum = casted.silhouetteSumInPartition;
				rowCount = casted.rowCountInPartition;
			}

			/*
			 * (non-Javadoc)
			 * 
			 * @see
			 * org.knime.core.node.streamable.StreamableOperator#saveInternals()
			 */
			@Override
			public StreamableInternal saveInternals() {
				final StreamableInternal ret = new StreamableInternal(
						StreamableInternal.State.COMPUTING_SILHOUETTE_VALUES, Optional.empty());
				ret.statistics.putAll(clusterStats);
				ret.rowCountInPartition = rowCount;
				ret.silhouetteSumInPartition = silhouetteSum;
				return ret;
			}

			@Override
			public void runFinal(PortInput[] inputs, PortOutput[] outputs, ExecutionContext exec) throws Exception {
				// Do nothing, finishStreamableExecution(internals, exec, output) does the "hard" work.
//				((RowOutput) outputs[0])
//						.push(new DefaultRow(RowKey.createRowKey(0L), new DoubleCell(silhouetteSum / rowCount)));
			}
		};
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.knime.core.node.NodeModel#finishStreamableExecution(org.knime.core.
	 * node.streamable.StreamableOperatorInternals,
	 * org.knime.core.node.ExecutionContext,
	 * org.knime.core.node.streamable.PortOutput[])
	 */
	@Override
	public void finishStreamableExecution(StreamableOperatorInternals internals, ExecutionContext exec,
			PortOutput[] output) throws CanceledExecutionException, InterruptedException {
		final StreamableInternal casted = (StreamableInternal) internals;
		exec.checkCanceled();
		final RowOutput rowOutput = (RowOutput) output[0];
		CheckUtils.checkState(casted.result.isPresent(), "Result is not present");
		rowOutput.push(new DefaultRow(RowKey.createRowKey(0L), new DoubleCell(casted.result.get())));
		rowOutput.close();
	}

	// /*
	// * (non-Javadoc)
	// *
	// * @see
	// *
	// org.knime.core.node.NodeModel#computeFinalOutputSpecs(org.knime.core.node
	// * .streamable.StreamableOperatorInternals,
	// * org.knime.core.node.port.PortObjectSpec[])
	// */
	// @Override
	// public PortObjectSpec[]
	// computeFinalOutputSpecs(StreamableOperatorInternals internals,
	// PortObjectSpec[] inSpecs)
	// throws InvalidSettingsException {
	// }

	private double computeSilhouetteAverage(final Map<String, ClusterStats> statistics, final DataTableRowInput rows,
			SortedMap<String, Integer> colIndices, Map<String, double[]> clusterCenters, int labelIdx,
			final ExecutionContext exec) throws CanceledExecutionException, InterruptedException {
		final Mean mean = new Mean();
		final double[] values = new double[colIndices.size()];
		final String[] clusterNames = clusterCenters.entrySet().stream().map(e -> e.getKey()).sorted()
				.toArray(n -> new String[n]);
		final double[][] prototypes = new double[clusterNames.length][];
		for (int i = clusterNames.length; i-- > 0;) {
			prototypes[i] = clusterCenters.get(clusterNames[i]);
		}
		DataRow row;
		while ((row = rows.poll()) != null) {
			exec.checkCanceled();
			double silhouette = computeSilhouette(statistics, colIndices, labelIdx, values, clusterNames, prototypes,
					row);
			mean.increment(silhouette);
		}
		return mean.getResult();
	}

	/**
	 * @param statistics
	 * @param colIndices
	 * @param labelIdx
	 * @param values
	 * @param clusterNames
	 * @param prototypes
	 * @param row
	 * @return
	 */
	private double computeSilhouette(final Map<String, ClusterStats> statistics, SortedMap<String, Integer> colIndices,
			int labelIdx, final double[] values, final String[] clusterNames, final double[][] prototypes,
			DataRow row) {
		int i = 0;
		for (final Entry<String, Integer> entry : colIndices.entrySet()) {
			DataCell cell = row.getCell(entry.getValue());
			values[i++] = ((DoubleValue) cell).getDoubleValue();
		}
		final String label = labelIdx >= 0 ? row.getCell(labelIdx).toString()
				: clusterNames[closestLabelIndex(values, prototypes)];
		final double sumSquare = Arrays.stream(values).map(FastSilhouetteNodeModel::sqr).sum();
		double closestOther = Double.POSITIVE_INFINITY;
		for (final Entry<String, ClusterStats> entry : statistics.entrySet()) {
			if (!entry.getKey().equals(label)) {
				final double distance = distance(sumSquare, values, entry.getValue());
				closestOther = Math.min(closestOther, distance);
			}
		}
		final ClusterStats cluster = statistics.get(label);
		final double ownDistance = cluster.n == 1L ? 0d
				: distance(sumSquare, values, cluster) * cluster.n / (cluster.n - 1);
		double silhouette = (ownDistance == closestOther) ? 0d
				: (ownDistance < closestOther) ? (1 - ownDistance / closestOther) : (closestOther / ownDistance - 1);
		return silhouette;
	}

	private static double distance(final Double sumSquare, final double[] point, final ClusterStats stats) {
		double yTimesPoint = 0d;
		for (int i = 0; i < point.length; i++) {
			yTimesPoint += point[i] * stats.y[i];
		}
		return sumSquare + (stats.psi - 2 * yTimesPoint) / stats.n;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.knime.core.node.NodeModel#iterate(org.knime.core.node.streamable.
	 * StreamableOperatorInternals)
	 */
	@Override
	public boolean iterate(StreamableOperatorInternals internals) {
		final StreamableInternal casted = (StreamableInternal) internals;
		return !casted.result.isPresent();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.knime.core.node.NodeModel#getInputPortRoles()
	 */
	@Override
	public InputPortRole[] getInputPortRoles() {
		return new InputPortRole[] { InputPortRole.NONDISTRIBUTED_NONSTREAMABLE,
				// Only single result is returned, no need to be streamable.
				InputPortRole.DISTRIBUTED_STREAMABLE };
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.knime.core.node.NodeModel#getOutputPortRoles()
	 */
	@Override
	public OutputPortRole[] getOutputPortRoles() {
		// Single result, no need to be streamable.
		return new OutputPortRole[] { OutputPortRole.NONDISTRIBUTED };
	}

	private Pair<Map<String, Pair<ClusterStats, double[]>>, SortedMap<String, Integer>> clusterStats(
			final RowInput data, PortObjectInput pmml, final ExecutionMonitor exec)
			throws InterruptedException, CanceledExecutionException, InvalidSettingsException {
		final PMMLPortObject pmmlPo = (PMMLPortObject) pmml.getPortObject();
		final PMMLClusterTranslator translator = new PMMLClusterTranslator();
		pmmlPo.initializeModelTranslator(translator);
		final List<Node> models = pmmlPo.getPMMLValue().getModels(PMMLModelType.ClusteringModel);
		CheckUtils.checkArgument(models.size() == 1, "Only a single clustering model is supported yet.");
		final SortedSet<String> usedColumns = new TreeSet<>(translator.getUsedColumns());
		final SortedMap<String, Integer> colIndices = new TreeMap<>();
		for (final String col : usedColumns) {
			final int idx = data.getDataTableSpec().findColumnIndex(col);
			CheckUtils.checkArgument(idx >= 0, "Not found column: " + col);
			colIndices.put(col, idx);
		}
		int labelIdx = -1;
		if (labelOpt.isEnabled() && labelOpt.getColumnName() != null) {
			labelIdx = data.getDataTableSpec().findColumnIndex(labelOpt.getColumnName());
			CheckUtils.checkSetting(labelIdx > -1, "Could not find labelOpt column: " + labelOpt.getColumnName());
		}
		final ComparisonMeasure comparisonMeasure = translator.getComparisonMeasure();
		switch (comparisonMeasure) {
		case squaredEuclidean:
			break;
		case euclidean:
			if (labelIdx < 0) {
				throw new UnsupportedOperationException("Not supported measure: Euclidean");
			}
			break;
		default:
			throw new UnsupportedOperationException("Not supported measure: " + comparisonMeasure);
		}
		final Map<String, Long> labelCount = new HashMap<>();
		final Map<String, Double> sumSquares = new HashMap<>();
		final Map<String, double[]> yss = new HashMap<>();

		final String[] labels = translator.getLabels();
		final double[][] prototypes = translator.getPrototypes();
		double[] values = new double[colIndices.size()];
		DataRow row;
		while ((row = data.poll()) != null) {
			exec.checkCanceled();
			final Iterator<String> it = usedColumns.iterator();
			for (int i = 0; i < values.length; i++) {
				final String col = it.next();
				CheckUtils.checkState(colIndices.containsKey(col), "Unknown column: " + col);
				final int index = colIndices.get(col);
				final DataCell cell = row.getCell(index);
				CheckUtils.checkArgument(!cell.isMissing() && cell instanceof DoubleValue,
						"Missing or non-Double value in " + row + " at " + cell);
				values[i] = ((DoubleValue) cell).getDoubleValue();
			}
			final String label = labelIdx >= 0 ? ((NominalValue) row.getCell(labelIdx)).toString()
					: labels[closestLabelIndex(values, prototypes)];
			labelCount.put(label, labelCount.getOrDefault(label, 0L) + 1L);
			final double sumSquare = Arrays.stream(values).map(d -> sqr(d)).sum();
			sumSquares.put(label, sumSquares.getOrDefault(label, 0d) + sumSquare);
			if (yss.containsKey(label)) {
				double[] ys = yss.get(label);
				for (int i = values.length; i-- > 0;) {
					ys[i] += values[i];
				}
			} else {
				yss.put(label, values.clone());
			}
		}
		final Map<String, Pair<ClusterStats, double[]>> ret = new HashMap<>();
		for (int l = 0; l < labels.length; l++) {
			final String label = labels[l];
			ret.put(label,
					Pair.create(
							new ClusterStats(yss.getOrDefault(label, new double[yss.values().iterator().next().length]),
									sumSquares.getOrDefault(label, 0d), labelCount.getOrDefault(label, 0L)),
							prototypes[l]));
		}
		return Pair.create(ret, colIndices);
	}

	/**
	 * @param values
	 * @param prototypes
	 * @return
	 */
	private int closestLabelIndex(double[] values, double[][] prototypes) {
		int closest = -1;
		double minDistance = Double.POSITIVE_INFINITY;
		for (int i = prototypes.length; i-- > 0;) {
			double dist = 0;
			for (int j = values.length; j-- > 0;) {
				dist += sqr(prototypes[i][j] - values[j]);
			}
			if (dist <= minDistance) {
				minDistance = dist;
				closest = i;
			}
		}
		return closest;
	}

	private static double sqr(Double d) {
		return d * d;
	}
}
