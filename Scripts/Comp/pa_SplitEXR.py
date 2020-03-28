
import os
import logging
import sys

logger = logging.getLogger('pa.splitexr')

VERSION = "0.0.0"

#
#   Logging
#


class LessThanFilter(logging.Filter):
    def __init__(self, exclusive_maximum, name=""):
        super(LessThanFilter, self).__init__(name)
        self.max_level = exclusive_maximum

    def filter(self, record):
        # non-zero return means we log this message
        return 1 if record.levelno < self.max_level else 0


def logConfig(debug_logger_name=None):
    """
        Initialize stream logger that prints to stdout and stderr which Fusion likes.
        If specified, set the given logger to DEBUG
    """
    # All logging limited to INFO only, except the given name
    logging.getLogger().setLevel(logging.INFO)
    # Set specific name to debug
    if debug_logger_name:
        logging.getLogger(debug_logger_name).setLevel(logging.DEBUG)
    # Make a handler for non-error messages
    h1 = logging.StreamHandler(sys.stdout)
    h1.setLevel(logging.DEBUG)
    # Filter out errors
    h1.addFilter(LessThanFilter(logging.WARNING))
    # Make a handler for error messages
    h2 = logging.StreamHandler(sys.stderr)
    h2.setLevel(logging.WARNING)
    logging.basicConfig(handlers=[h1, h2])

#
#   Util functions
#


def get_selected_tools(comp, tool_type=None):
    """
        Returns a list of selected tools
    """
    return list(comp.GetToolList(True, tool_type).values())


def get_tool_name(tool):
    """
        Get tool name
    """
    return tool.GetAttrs()["TOOLS_Name"]


def set_tool_name(tool, name):
    """
        Set a tool name
    """
    tool.SetAttrs({
        "TOOLB_NameSet": True,
        "TOOLS_Name": name
    })


def is_multipart_exr(tool):
    """
        Return True if this is a Multi-Part EXR Loader
    """

    if not is_exr_loader(tool):
        return False

    return tool.Clip1.OpenEXRFormat.Part is not None


def is_exr_loader(tool):
    """
        Return True if this is an EXR loader
    """
    attrs = tool.GetAttrs()
    fmt = attrs["TOOLST_Clip_FormatName"][1]
    return fmt == "OpenEXRFormat"


def get_or_default(key, options, default_options):
    return options.get(key, default_options[key])


#
#   Plugins
#

class EXRSplitPlugin(object):

    default_options = {}

    def __init__(self, exr_filename, loader, layers, options={}):
        self.exr_filename = exr_filename
        self.loader = loader
        self.layers = layers
        self.options = options
        for key in self.default_options:
            if not key in self.options:
                self.options[key] = self.default_options[key]

    def process_layer(self, layer_name, channels):
        """
            Process an EXR 'layer' which is a grouping of EXR channels.

            :param layer_name: The name of the layer
            :type layer_name: str

            :param channels: List of 2-tuples describing channels in the layer.  First item is the name without prefix, second is its full 'raw' channel name.
            :type channels: list

            :returns: False if layer could not be processed by this plugin.  Any other value to prevent other plugins from processing, optionally return a new tool to be arranged.
        """
        raise NotImplementedError()


class DefaultPlugin(EXRSplitPlugin):

    """
        The default plugin that creates a new Loader from a layer.

        This creates a new loader with RGBA channels assigned, but no aux channels used.

        It will attempt to remap XYZ to RGB, and UVW to RGB.

        Options:
            - alpha_fallback
                If no alpha channel is found on the given layer, attempt to use the default alpha channel.

    """

    default_options = {
        'alpha_fallback': False
    }

    channel_mappings = {
        'r': 'RedName',
        'g': 'GreenName',
        'b': 'BlueName',
        'a': 'AlphaName',
        'u': 'RedName',
        'v': 'GreenName',
        'w': 'BlueName',
        'x': 'RedName',
        'y': 'GreenName',
        'z': 'BlueName'
    }

    CHANNEL_NO_MATCH = "SomethingThatWontMatchHopefully"

    def process_layer(self, layer_name, channels):

        logger.info('Creating new loader for layer: %s', layer_name)

        new_loader = comp.Loader({
            "Clip": self.exr_filename
        })

        fmt = new_loader.Clip1.OpenEXRFormat

        fmt.RedName = self.CHANNEL_NO_MATCH
        fmt.GreenName = self.CHANNEL_NO_MATCH
        fmt.BlueName = self.CHANNEL_NO_MATCH
        fmt.AlphaName = self.CHANNEL_NO_MATCH
        fmt.ZName = self.CHANNEL_NO_MATCH

        for channel, raw_channel in channels:
            if not channel in self.channel_mappings:
                logger.error('Unknown channel: {}'.format(channel))
                continue
            mapping = self.channel_mappings[channel]
            fmt[mapping] = raw_channel

        if self.options['alpha_fallback'] and not 'a' in channels.keys():
            fallback_alpha_channel = self.layers.get('default', {}).get('a', None)
            if fallback_alpha_channel:
                fmt['AlphaName'] = fallback_alpha_channel

        new_loader.GlobalIn = self.loader.GlobalIn[0]
        new_loader.GlobalOut = self.loader.GlobalOut[0]
        name = layer_name
        set_tool_name(new_loader, name)

        return new_loader


class CryptomattePlugin(EXRSplitPlugin):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.made_crypto = False

    def process_layer(self, layer_name, channels):
        if not "crypto" in layer_name.lower():
            return False

        logger.info("Cryptomatte: %s", layer_name)
        if self.made_crypto:
            return True

        logger.info("Creating Cryptomatte node.")
        new_tool = comp.AddTool("Fuse.Cryptomatte")
        new_tool.Input.ConnectTo(self.loader)
        exr_name, _ = os.path.splitext(os.path.basename(self.exr_filename))
        name = "Cryp_{}".format(exr_name)
        set_tool_name(new_tool, name)
        self.made_crypto = True
        return new_tool


class VrayPlugin(EXRSplitPlugin):

    default_options = {
        'use_aux_channels': True,
        'flip_worldpos_axis': True
    }

    # Sometimes aux channels stored as RGB in the EXR, translate to XYZ
    pos_remappings = {
        'r': 'x',
        'g': 'y',
        'b': 'z'
    }

    # Mappings of layer names to native Fusion channel groupings
    layer_name_mappings = {
        'normal': 'NormName',
        'pos': 'PosName',
        'vel': 'VelName',
    }

    def process_layer(self, layer_name, channels):
        # This plugin only deals with aux channels.
        # In the future it may do other things too.
        if not self.options['use_aux_channels']:
            return False

        logger.info('Native Fusion Layer: %s', layer_name)
        fmt = self.loader.Clip1.OpenEXRFormat
        mapping_name = next((l for l in self.layer_name_mappings if l in layer_name.lower()), None)
        if not mapping_name:
            return False

        mapping = self.layer_name_mappings[mapping_name]

        for channel, raw_channel in channels:
            # Translate RGB to XYZ if necessary
            channel = self.pos_remappings.get(channel, channel)
            if mapping_name == 'pos':
                logger.info('World Position: %s', layer_name)
                # World Position Tweaks
                if self.options['flip_worldpos_axis']:
                    logger.info('Doing 3DSMax WorldPos tweaks for channel: %s', channel)
                    if channel == 'z':
                        logger.info('Flipping Z axis to Y')
                        channel = 'y'
                    elif channel == 'y':
                        logger.info('Flipping Y axis to Z')
                        channel = 'z'  # Note that Z channel needs to be inverted downstream.  Not able to do that here.
            channel = channel.upper()
            fusion_name = '{}{}'.format(channel, mapping)
            logger.info('{} -> {}'.format(fusion_name, raw_channel))
            fmt[fusion_name] = raw_channel

        return True

#
#   Main EXR split functions
#


def split_multilayer_exr(comp, loader, plugin_classes=[], options={}):

    namespace_separator = "."

    attrs = loader.GetAttrs()

    exr_filename = attrs["TOOLST_Clip_Name"][1]

    # Get all loader channel and filter out the ones to skip
    sourceChannels = loader.Clip1.OpenEXRFormat.RedName.GetAttrs()["INPIDT_ComboControl_ID"].values()

    # Create dict where key is the "layer name" and value is list of tuples where first lement
    # is the channel name (R,G,B, etc) and second item is the raw channel name (VraySomething.R etc)
    layers = {}

    for channel in sourceChannels:
        s = channel.split(namespace_separator)
        if len(s) > 1:
            key = s[-1]
            layer_name = namespace_separator.join(s[0:-1])
        else:
            key = channel
            layer_name = 'default'
        if not layer_name in layers:
            layers[layer_name] = []
        key = key.lower()
        layers[layer_name].append((key, channel))

    logger.debug('%s layers found.', len(layers))

    plugins = [p(exr_filename, loader, layers, options) for p in plugin_classes]

    new_tools = []

    for layer_name, channels in layers.items():
        logger.info("Layer: %s", layer_name)
        """
            Iterate over plugin hooks, finding the first one that returns a new tool.
        """
        for plugin in plugins:
            logger.debug('Attempting to use plugin %s on layer %s', plugin.__class__.__name__, layer_name)
            result = plugin.process_layer(layer_name, channels)
            logger.debug('Result: {}'.format(result))  # Note: must use format here because result my not have __repr__ defined
            if result:
                logger.debug('There was a result.')
                if hasattr(result, 'GetAttrs'):
                    logger.debug('The result was a new tool.')
                    new_tools.append(result)
                logger.debug('Plugin %s successfully processed layer %s', plugin.__class__.__name__, layer_name)
                break
            else:
                logger.debug('Plugin %s rejected layer %s', plugin.__class__.__name__, layer_name)

    logger.debug('Finished processing all layers.')

    return new_tools


def split_multipart_exr(comp, tool, plugin_classes, options):
    """
        Split a Multi-Part EXR
    """

    if not is_exr_loader(tool):
        return

    attrs = tool.GetAttrs()

    exr_filename = attrs["TOOLST_Clip_Name"][1]

    inp = tool.Clip1.OpenEXRFormat.Part

    channel_table = inp.GetAttrs()["INPIDT_ComboControl_ID"]

    new_tools = []

    for i, name in channel_table.items():
        new_loader = comp.Loader({
            "Clip": exr_filename
        })
        new_loader.SetAttrs({"TOOLB_NameSet": True, "TOOLS_Name": name})
        new_loader.Gamut.GammaSpace = tool.Gamut.GammaSpace
        new_loader.Clip1.OpenEXRFormat.Part = name
        new_loader.GlobalIn = tool.GlobalIn[0]
        new_loader.GlobalOut = tool.GlobalOut[0]

        new_tools.append(new_loader)

    return new_tools


def arrange_tools_table(comp, tools, col_spacing=1, max_rows=20, origin_tool=None):
    flow = comp.CurrentFrame.FlowView
    if not origin_tool:
        origin_tool = tools[0]
    org_x_pos, org_y_pos = flow.GetPosTable(origin_tool).values()

    max_rows = 20

    for i, new_loader in enumerate(tools):

        col = int(i / max_rows)
        row = int(i % max_rows)

        flow.SetPos(new_loader, org_x_pos + col * col_spacing, org_y_pos + row)


def split_exr(comp, tool, plugin_classes, options):
    if is_multipart_exr(tool):
        new_tools = split_multipart_exr(comp, tool, plugin_classes, options)
    elif is_exr_loader(tool):
        new_tools = split_multilayer_exr(comp, tool, plugin_classes, options)
    if not new_tools:
        return
    arrange_tools_table(comp, new_tools, origin_tool=tool)


def split_exr_script(comp):
    logger.debug("Attempting to split EXR from selected tools.")
    options = {}
    plugin_classes = [CryptomattePlugin, DefaultPlugin]  # Note: always put the default plugin last.
    comp.StartUndo("Split EXR")
    comp.Lock()
    try:
        for tool in get_selected_tools(comp, "Loader"):
            logger.debug("Tool: {}".format(get_tool_name(tool)))
            split_exr(comp, tool, plugin_classes, options)
    finally:
        comp.Unlock()
        comp.EndUndo(True)


if __name__ == "__main__":
    logConfig(debug_logger_name='pa')
    if not comp:
        logger.error("No comp active.")
        sys.exit(1)
    split_exr_script(comp)
