// Copyright 2019 Esri

// Licensed under the Apache License, Version 2.0 (the "License");

// you may not use this file except in compliance with the License.

// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software

// distributed under the License is distributed on an "AS IS" BASIS,

// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

// See the License for the specific language governing permissions and

// limitations under the License.​

import WebMap = require("esri/WebMap");
import MapView = require("esri/views/MapView");
import FeatureLayer = require("esri/layers/FeatureLayer");
import DotDensityRenderer = require("esri/renderers/DotDensityRenderer");
import Legend = require("esri/widgets/Legend");
import Bookmarks = require("esri/widgets/Bookmarks");
import Search = require("esri/widgets/Search")
import Expand = require("esri/widgets/Expand");

( async () => {

  const map = new WebMap({
    portalItem: {
      id: "da83595b291349b79c7e56e5fabc5fde"
    }
  });

  const view = new MapView({
    container: "viewDiv",
    map: map,
    highlightOptions: {
      fillOpacity: 0,
      color: "white"
    },
    popup: {
      dockEnabled: true,
      dockOptions: {
        position: "top-right",
        breakpoint: false
      }
    },
    constraints: {
      maxScale: 35000
    }
  });

  await view.when();
  const dotDensityRenderer = new DotDensityRenderer({
    referenceDotValue: 100,
    outline: null,
    referenceScale: 577790,
    legendOptions: {
      unit: "people"
    },
    attributes: [
      {
        field: "B03002_003E",
        color: "#f23c3f",
        label: "White (non-Hispanic)"
      },
      {
        field: "B03002_012E",
        color: "#e8ca0d",
        label: "Hispanic"
      },
      {
        field: "B03002_004E",
        color: "#00b6f1",
        label: "Black or African American"
      },
      {
        field: "B03002_006E",
        color: "#32ef94",
        label: "Asian"
      },
      {
        field: "B03002_005E",
        color: "#ff7fe9",
        label: "American Indian/Alaskan Native"
      },
      {
        field: "B03002_007E",
        color: "#e2c4a5",
        label: "Pacific Islander/Hawaiian Native"
      },
      {
        field: "B03002_008E",
        color: "#ff6a00",
        label: "Other race"
      },
      {
        field: "B03002_009E",
        color: "#96f7ef",
        label: "Two or more races"
      }
    ]
  });

  // Add renderer to the layer and define a popup template
  const url = "https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services/ACS_Population_by_Race_and_Hispanic_Origin_Boundaries/FeatureServer/2";
  const layer = new FeatureLayer({
    url: url,
    minScale: 20000000,
    maxScale: 35000,
    title: "Current Population Estimates (ACS)",
    popupTemplate: {
      title: "{County}, {State}",
      content: [
        {
          type: "fields",
          fieldInfos: [
            {
              fieldName: "B03002_003E",
              label: "White (non-Hispanic)",
              format: {
                digitSeparator: true,
                places: 0
              }
            },
            {
              fieldName: "B03002_012E",
              label: "Hispanic",
              format: {
                digitSeparator: true,
                places: 0
              }
            },
            {
              fieldName: "B03002_004E",
              label: "Black or African American",
              format: {
                digitSeparator: true,
                places: 0
              }
            },
            {
              fieldName: "B03002_006E",
              label: "Asian",
              format: {
                digitSeparator: true,
                places: 0
              }
            },
            {
              fieldName: "B03002_005E",
              label: "American Indian/Alaskan Native",
              format: {
                digitSeparator: true,
                places: 0
              }
            },
            {
              fieldName: "B03002_007E",
              label: "Pacific Islander/Hawaiian Native",
              format: {
                digitSeparator: true,
                places: 0
              }
            },
            {
              fieldName: "B03002_008E",
              label: "Other race",
              format: {
                digitSeparator: true,
                places: 0
              }
            },
            {
              fieldName: "B03002_009E",
              label: "Two or more races",
              format: {
                digitSeparator: true,
                places: 0
              }
            }
          ]
        }
      ]
    },
    renderer: dotDensityRenderer
  });

  map.add(layer);

  const legendContainer = document.getElementById("legendDiv");
  const legend = new Legend({ 
    view,
    container: legendContainer
  });

  view.ui.add([
    new Expand({
      view,
      content: document.getElementById("controlDiv"),
      group: "top-left",
      expanded: true,
      expandIconClass: "esri-icon-layer-list"
    }),
    new Expand({
      view,
      expandIconClass: "esri-icon-filter",
      content: document.getElementById("sliderDiv"),
      group: "top-left"
    }),
    new Expand({
      view,
      content: new Search({ view }),
      group: "top-left"
    })
  ], "top-left" );

  view.ui.add(
    new Expand({
      view,
      content: new Bookmarks({ view }),
      group: "bottom-right",
      expanded: true
    }), "bottom-right");

  legendContainer.addEventListener("mousemove", legendEventListener);
  legendContainer.addEventListener("click", legendEventListener);

  let mousemoveEnabled = true;

  // enables exploration on mouse move
  const resetButton = document.getElementById("reset-button") as HTMLButtonElement;
  resetButton.addEventListener("click", () => {
    mousemoveEnabled = true;
    layer.renderer = dotDensityRenderer;
    legendContainer.addEventListener("mousemove", legendEventListener);
  });

  function legendEventListener (event:any) {
    const selectedText =   event.target.alt || event.target.innerText;
    const legendInfos: Array<any> = legend.activeLayerInfos.getItemAt(0).legendElements[0].infos;
    const matchFound = legendInfos.filter( (info:any) => info.label === selectedText ).length > 0;
    if (matchFound){
      showSelectedField(selectedText);
      if (event.type === "click"){
        mousemoveEnabled = false;
        legendContainer.removeEventListener("mousemove", legendEventListener);
      } 
    } else {
      layer.renderer = dotDensityRenderer;
    }
  }

  function showSelectedField (label: string) {
    const oldRenderer = layer.renderer as DotDensityRenderer;
    const newRenderer = oldRenderer.clone();
    const attributes = newRenderer.attributes.map( attribute => {
      attribute.color.a = attribute.label === label ? 1 : 0.2;
      return attribute;
    });
    newRenderer.attributes = attributes;
    layer.renderer = newRenderer;
  }

  const dotValueSlider = document.getElementById("dotValueInput") as HTMLInputElement;
  const dotValueDisplay = document.getElementById("dotValueDisplay") as HTMLSpanElement;
  dotValueSlider.addEventListener("input", () => {
    const oldRenderer = layer.renderer as DotDensityRenderer;
    const newRenderer = oldRenderer.clone();
    dotValueDisplay.innerText = dotValueSlider.value;
    const dotValue = parseInt(dotValueSlider.value);
    newRenderer.referenceDotValue = dotValue;
    layer.renderer = newRenderer;
  });

})();