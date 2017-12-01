package com.mind_era.knime.silhouette;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "FastSilhouette" Node.
 * Allows computing the squared Euclidean <a href="https://en.wikipedia.org/wiki/Silhouette_(clustering)">Silhouette</a> (score of a clustering) as a distributed algorithm.
 *
 * @author Mind Eratosthenes Kft.
 */
public class FastSilhouetteNodeFactory 
        extends NodeFactory<FastSilhouetteNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public FastSilhouetteNodeModel createNodeModel() {
        return new FastSilhouetteNodeModel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNrNodeViews() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeView<FastSilhouetteNodeModel> createNodeView(final int viewIndex,
            final FastSilhouetteNodeModel nodeModel) {
    	throw new IndexOutOfBoundsException("No views are present, but requested: " + viewIndex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasDialog() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeDialogPane createNodeDialogPane() {
        return new FastSilhouetteNodeDialog();
    }

}

