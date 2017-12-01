package com.mind_era.knime.silhouette;

import org.knime.core.data.NominalValue;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;

/**
 * <code>NodeDialog</code> for the "FastSilhouette" Node. Allows computing the
 * squared Euclidean <a href=
 * "https://en.wikipedia.org/wiki/Silhouette_(clustering)">Silhouette</a> (score
 * of a clustering) as a distributed algorithm.
 *
 * This node dialog derives from {@link DefaultNodeSettingsPane} which allows
 * creation of a simple dialog with standard components. If you need a more
 * complex dialog please derive directly from
 * {@link org.knime.core.node.NodeDialogPane}.
 * 
 * @author Mind Eratosthenes Kft.
 */
class FastSilhouetteNodeDialog extends DefaultNodeSettingsPane {

	/**
	 * New pane for configuring the FastSilhouette node.
	 */
	@SuppressWarnings("unchecked")
	protected FastSilhouetteNodeDialog() {
		addDialogComponent(new DialogComponentStringSelection(FastSilhouetteNodeModel.createMeasureSettings(),
				"Measure: ", FastSilhouetteNodeModel.SUPPORTED_MEASURES));
		addDialogComponent(new DialogComponentColumnNameSelection(
				FastSilhouetteNodeModel.createOptionalLabelColumnSettings(), "Label", FastSilhouetteNodeModel.DATA_INDEX, false, true, NominalValue.class));
	}
}
